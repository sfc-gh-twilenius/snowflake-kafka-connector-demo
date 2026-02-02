#!/usr/bin/env python3
"""
Financial Transaction Data Producer for Kafka
Generates realistic mock financial transaction data and streams it to Kafka.
"""

import json
import time
import uuid
import random
import argparse
import signal
import sys
from datetime import datetime, timezone
from typing import Optional

from kafka import KafkaProducer
from faker import Faker

fake = Faker()

# Configuration
TRANSACTION_TYPES = ['purchase', 'withdrawal', 'transfer', 'deposit', 'refund']
CURRENCIES = ['USD', 'EUR', 'GBP', 'CHF', 'JPY', 'CAD', 'AUD']
STATUSES = ['completed', 'pending', 'failed']
STATUS_WEIGHTS = [0.85, 0.10, 0.05]  # 85% completed, 10% pending, 5% failed

MERCHANT_CATEGORIES = [
    'Grocery', 'Restaurant', 'Gas Station', 'Online Shopping', 
    'Entertainment', 'Travel', 'Healthcare', 'Utilities',
    'Electronics', 'Clothing', 'Home Improvement', 'Insurance',
    'Subscription Services', 'Financial Services', 'Education'
]

CARD_NETWORKS = ['Visa', 'Mastercard', 'Amex', 'Discover']


class FinancialDataGenerator:
    """Generates realistic financial transaction data."""
    
    def __init__(self):
        self.fake = Faker()
        # Pre-generate some account IDs to simulate repeat customers
        self.account_pool = [self._generate_account_id() for _ in range(100)]
        self.merchant_pool = [self._generate_merchant() for _ in range(50)]
    
    def _generate_account_id(self) -> str:
        """Generate a realistic account ID."""
        return f"ACC-{random.randint(100000, 999999)}-{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}"
    
    def _generate_merchant(self) -> dict:
        """Generate merchant data."""
        category = random.choice(MERCHANT_CATEGORIES)
        return {
            'merchant_id': f"MER-{uuid.uuid4().hex[:8].upper()}",
            'merchant_name': self.fake.company(),
            'merchant_category': category,
            'mcc_code': str(random.randint(1000, 9999))
        }
    
    def generate_transaction(self) -> dict:
        """Generate a single financial transaction."""
        transaction_type = random.choice(TRANSACTION_TYPES)
        currency = random.choice(CURRENCIES)
        
        # Amount varies by transaction type
        if transaction_type == 'purchase':
            amount = round(random.uniform(1.00, 500.00), 2)
        elif transaction_type == 'withdrawal':
            amount = round(random.choice([20, 40, 50, 60, 80, 100, 200, 300, 500]), 2)
        elif transaction_type == 'transfer':
            amount = round(random.uniform(10.00, 5000.00), 2)
        elif transaction_type == 'deposit':
            amount = round(random.uniform(100.00, 10000.00), 2)
        else:  # refund
            amount = round(random.uniform(5.00, 200.00), 2)
        
        # Use existing account or generate new one (80% existing, 20% new)
        if random.random() < 0.8:
            account_id = random.choice(self.account_pool)
        else:
            account_id = self._generate_account_id()
            self.account_pool.append(account_id)
            if len(self.account_pool) > 200:
                self.account_pool.pop(0)
        
        # Generate location
        location = {
            'city': self.fake.city(),
            'country': self.fake.country_code(),
            'latitude': float(self.fake.latitude()),
            'longitude': float(self.fake.longitude())
        }
        
        # Use existing merchant or generate new one
        if transaction_type in ['purchase', 'refund']:
            if random.random() < 0.7:
                merchant = random.choice(self.merchant_pool)
            else:
                merchant = self._generate_merchant()
                self.merchant_pool.append(merchant)
                if len(self.merchant_pool) > 100:
                    self.merchant_pool.pop(0)
        else:
            merchant = None
        
        # Card info (only for card-based transactions)
        card_info = None
        if transaction_type in ['purchase', 'withdrawal', 'refund']:
            card_info = {
                'card_last_four': str(random.randint(1000, 9999)),
                'card_network': random.choice(CARD_NETWORKS),
                'card_type': random.choice(['debit', 'credit'])
            }
        
        transaction = {
            'transaction_id': str(uuid.uuid4()),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'account_id': account_id,
            'transaction_type': transaction_type,
            'amount': amount,
            'currency': currency,
            'status': random.choices(STATUSES, weights=STATUS_WEIGHTS)[0],
            'location': location,
            'merchant': merchant,
            'card_info': card_info,
            'metadata': {
                'channel': random.choice(['mobile_app', 'web', 'pos_terminal', 'atm', 'branch']),
                'device_id': uuid.uuid4().hex[:16] if random.random() > 0.3 else None,
                'ip_address': self.fake.ipv4() if random.random() > 0.4 else None
            }
        }
        
        # Add transfer-specific fields
        if transaction_type == 'transfer':
            transaction['recipient_account_id'] = self._generate_account_id()
            transaction['transfer_reference'] = f"TRF-{uuid.uuid4().hex[:10].upper()}"
        
        return transaction


class KafkaTransactionProducer:
    """Produces financial transactions to Kafka."""
    
    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.generator = FinancialDataGenerator()
        self.running = False
        self.message_count = 0
        
        print(f"Connecting to Kafka at {bootstrap_servers}...")
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3
        )
        print("Connected to Kafka successfully!")
    
    def send_transaction(self, transaction: dict) -> None:
        """Send a single transaction to Kafka."""
        key = transaction['account_id']
        self.producer.send(self.topic, key=key, value=transaction)
        self.message_count += 1
    
    def run(self, rate: float = 1.0, max_messages: Optional[int] = None) -> None:
        """
        Run the producer continuously.
        
        Args:
            rate: Transactions per second
            max_messages: Maximum number of messages to send (None for unlimited)
        """
        self.running = True
        interval = 1.0 / rate
        
        print(f"Starting to produce transactions at {rate} TPS to topic '{self.topic}'")
        print("Press Ctrl+C to stop...")
        
        try:
            while self.running:
                if max_messages and self.message_count >= max_messages:
                    print(f"\nReached maximum message count: {max_messages}")
                    break
                
                transaction = self.generator.generate_transaction()
                self.send_transaction(transaction)
                
                if self.message_count % 10 == 0:
                    print(f"\rProduced {self.message_count} transactions...", end='', flush=True)
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\n\nShutting down gracefully...")
        finally:
            self.stop()
    
    def run_burst(self, count: int) -> None:
        """Send a burst of transactions as fast as possible."""
        print(f"Sending burst of {count} transactions...")
        
        for i in range(count):
            transaction = self.generator.generate_transaction()
            self.send_transaction(transaction)
            
            if (i + 1) % 100 == 0:
                print(f"\rSent {i + 1}/{count} transactions...", end='', flush=True)
        
        self.producer.flush()
        print(f"\nBurst complete! Sent {count} transactions.")
    
    def stop(self) -> None:
        """Stop the producer and flush remaining messages."""
        self.running = False
        print(f"Flushing remaining messages...")
        self.producer.flush()
        self.producer.close()
        print(f"Producer stopped. Total messages sent: {self.message_count}")


def main():
    parser = argparse.ArgumentParser(
        description='Generate and stream financial transaction data to Kafka'
    )
    parser.add_argument(
        '--bootstrap-servers', '-b',
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)'
    )
    parser.add_argument(
        '--topic', '-t',
        default='financial_transactions',
        help='Kafka topic name (default: financial_transactions)'
    )
    parser.add_argument(
        '--rate', '-r',
        type=float,
        default=1.0,
        help='Transactions per second (default: 1.0)'
    )
    parser.add_argument(
        '--max-messages', '-m',
        type=int,
        default=None,
        help='Maximum number of messages to send (default: unlimited)'
    )
    parser.add_argument(
        '--burst', 
        type=int,
        default=None,
        help='Send a burst of N transactions and exit'
    )
    
    args = parser.parse_args()
    
    producer = KafkaTransactionProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic
    )
    
    # Handle graceful shutdown
    def signal_handler(sig, frame):
        producer.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    if args.burst:
        producer.run_burst(args.burst)
    else:
        producer.run(rate=args.rate, max_messages=args.max_messages)


if __name__ == '__main__':
    main()
