"""
TF-IDF based job title classifier for filtering relevant data/BI/analyst roles.

Trains on existing labeled jobs (all jobs in staging.jobs_v1 are assumed to be relevant)
and classifies new job titles with a confidence score.
"""

import json
import pickle
import os
from typing import Dict, List, Tuple
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline
import psycopg2
import numpy as np


class JobClassifier:
    """TF-IDF + Naive Bayes classifier for job titles."""
    
    def __init__(self, model_path: str = "job_classifier_model.pkl"):
        self.model_path = model_path
        self.model = None
        self.is_trained = False
        self.load_or_train()
    
    def load_or_train(self):
        """Load existing model or train new one from database."""
        if os.path.exists(self.model_path):
            print(f"📦 Loading existing model from {self.model_path}")
            with open(self.model_path, 'rb') as f:
                self.model = pickle.load(f)
            self.is_trained = True
        else:
            print("🎓 No model found. Training new classifier from database...")
            self.train_from_database()
    
    def train_from_database(self):
        """Train classifier on jobs in staging.jobs_v1 (positive) and create negative examples."""
        # Load database credentials
        with open('local.settings.json') as f:
            settings = json.load(f).get('Values', {})
        
        conn = psycopg2.connect(
            host=settings['PGHOST'],
            port=settings['PGPORT'],
            database=settings['PGDATABASE'],
            user=settings['PGUSER'],
            password=settings['PGPASSWORD'],
            sslmode=settings.get('PGSSLMODE', 'require')
        )
        
        cur = conn.cursor()
        # Get positive examples (relevant jobs in staging)
        cur.execute("SELECT job_title FROM staging.jobs_v1;")
        positive_titles = [row[0] for row in cur.fetchall()]
        
        # Get negative examples (jobs we deleted as false positives from landing.raw_jobs that aren't in staging)
        # Since we don't have a list of what we deleted, we'll use common false positive keywords
        negative_examples = [
            "Accountant", "Management Accountant", "Financial Accountant",
            "Account Manager", "Account Executive", "Account Handler",
            "Fabric Technician", "Fabric Engineer", "Fabric Supervisor",
            "Billing Assistant", "Billing Manager", "Billing Clerk",
            "Administrator", "Office Administrator", "System Administrator",
            "Receptionist", "HR Administrator", "Finance Administrator",
            "Sales Executive", "Business Development Manager", "Sales Manager",
            "Trainee Accountant", "Junior Accountant", "Accounts Assistant",
            "Fabric Maintenance Engineer", "Building Maintenance Engineer",
            "Fabric Cutter", "Fabric Supervisor", "Warehouse Manager"
        ]
        
        cur.close()
        conn.close()
        
        if not positive_titles:
            raise ValueError("No jobs found in database to train on!")
        
        # Combine positive and negative examples
        all_titles = positive_titles + negative_examples
        all_labels = np.concatenate([
            np.ones(len(positive_titles)),      # 1 = relevant
            np.zeros(len(negative_examples))     # 0 = not relevant
        ])
        
        print(f"📚 Training on {len(positive_titles):,} positive examples + {len(negative_examples)} negative examples")
        
        # Create pipeline: TF-IDF + Naive Bayes
        self.model = Pipeline([
            ('tfidf', TfidfVectorizer(
                lowercase=True,
                ngram_range=(1, 2),  # Unigrams + bigrams
                max_features=500,
                min_df=1,  # Allow terms that appear in 1+ documents
                max_df=0.9,  # Ignore terms that appear in > 90% of documents
                stop_words='english'
            )),
            ('clf', MultinomialNB(alpha=0.1))
        ])
        
        # Train the model
        self.model.fit(all_titles, all_labels)
        self.is_trained = True
        
        # Save model
        with open(self.model_path, 'wb') as f:
            pickle.dump(self.model, f)
        
        print(f"✅ Model trained and saved to {self.model_path}")
    
    def classify(self, job_title: str, threshold: float = 0.5) -> Tuple[bool, float]:
        """
        Classify a job title as relevant or not.
        
        Args:
            job_title: The job title to classify
            threshold: Confidence threshold (0-1). Titles with probability >= threshold are relevant.
        
        Returns:
            Tuple of (is_relevant, confidence_score)
        """
        if not self.is_trained:
            raise ValueError("Model not trained yet!")
        
        # Get prediction probability
        prob = self.model.predict_proba([job_title])[0]
        confidence = prob[1]  # Probability of positive class (relevant)
        is_relevant = confidence >= threshold
        
        return is_relevant, confidence
    
    def classify_batch(self, job_titles: List[str], threshold: float = 0.5) -> List[Tuple[bool, float]]:
        """
        Classify multiple job titles efficiently.
        
        Args:
            job_titles: List of job titles to classify
            threshold: Confidence threshold (0-1)
        
        Returns:
            List of (is_relevant, confidence_score) tuples
        """
        if not self.is_trained:
            raise ValueError("Model not trained yet!")
        
        probs = self.model.predict_proba(job_titles)
        results = []
        for prob in probs:
            confidence = prob[1]
            is_relevant = confidence >= threshold
            results.append((is_relevant, confidence))
        
        return results
    
    def retrain(self):
        """Retrain the model with latest data from database."""
        self.is_trained = False
        self.train_from_database()


if __name__ == "__main__":
    # Test the classifier
    classifier = JobClassifier()
    
    test_titles = [
        "Senior Data Engineer",
        "BI Developer",
        "Data Analyst",
        "Fabric Technician",
        "Accountant",
        "Management Consultant",
        "Business Intelligence Manager",
        "Power BI Developer",
        "Fabric Engineer",
        "Account Manager",
        "Building Maintenance Engineer"
    ]
    
    print("\n" + "=" * 70)
    print("CLASSIFIER TEST RESULTS (threshold=0.7)")
    print("=" * 70)
    
    threshold = 0.7
    for title in test_titles:
        is_relevant, confidence = classifier.classify(title, threshold=threshold)
        status = "✅ RELEVANT" if is_relevant else "❌ NOT RELEVANT"
        print(f"{confidence:.1%} | {status:20} | {title}")
    
    print("=" * 70 + "\n")
