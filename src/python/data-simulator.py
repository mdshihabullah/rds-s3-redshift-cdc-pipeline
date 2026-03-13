#!/usr/bin/env python3
"""
gaming_data_simulator.py
Continuous data simulator for gaming database using psycopg3 and Faker
Loads configuration from data-simulator.env file
"""

import os
import psycopg
from psycopg import sql
from psycopg.rows import dict_row
import random
import time
import sys
import json
from decimal import Decimal
from pathlib import Path
from faker import Faker
from dotenv import load_dotenv
import logging

# Load environment variables from data-simulator.env (resolved relative to this file)
BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / 'data-simulator.env'
load_dotenv(ENV_FILE)

# Initialize Faker
fake = Faker()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GamingDataSimulator:
    def __init__(self):
        # Load database configuration from environment
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'dbname': os.getenv('DB_NAME', 'postgres'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD'),
        }
        
        # Validate required environment variables
        if not self.db_config['host'] or not self.db_config['password']:
            logger.error("Missing required DB_HOST or DB_PASSWORD in data-simulator.env")
            sys.exit(1)
        
        # Database schema
        self.schema = os.getenv('DB_SCHEMA', 'gaming')
        
        # Simulation settings
        self.cycle_delay = float(os.getenv('CYCLE_DELAY', 0.5))
        self.stats_interval = int(os.getenv('STATS_INTERVAL', 30))
        
        # Simulation probabilities
        self.probabilities = {
            'new_user': float(os.getenv('PROB_NEW_USER', 0.05)),
            'update_user': float(os.getenv('PROB_UPDATE_USER', 0.15)),
            'start_session': float(os.getenv('PROB_START_SESSION', 0.10)),
            'end_session': float(os.getenv('PROB_END_SESSION', 0.05)),
            'generate_events': float(os.getenv('PROB_GENERATE_EVENTS', 0.40))
        }
        
        self.conn = None
        
        # Cache for active data
        self.users = []
        self.games = []
        self.active_sessions = {}
        
        # Configuration
        self.account_levels = ['bronze', 'silver', 'gold', 'platinum', 'diamond']
        self.device_types = ['PC', 'Mobile', 'Console', 'Browser', 'Tablet']
        self.event_types = [
            'kill', 'death', 'assist', 'score_update',
            'level_complete', 'item_collected', 'achievement_unlocked',
            'checkpoint_reached', 'quest_complete', 'powerup_used'
        ]
        
        # Statistics
        self.stats = {
            'users_created': 0,
            'users_updated': 0,
            'sessions_started': 0,
            'sessions_ended': 0,
            'events_created': 0,
            'total_operations': 0
        }
    
    def connect(self):
        """Establish database connection"""
        try:
            # Use keyword arguments instead of connection string to handle special chars
            self.conn = psycopg.connect(**self.db_config)

            with self.conn.cursor() as cur:
                cur.execute(
                    sql.SQL("SET search_path TO {}").format(
                        sql.Identifier(self.schema)
                    )
                )
            self.conn.commit()
            
            logger.info(f"✓ Connected to {self.db_config['host']}")
            self.load_existing_data()
        except Exception as e:
            logger.error(f"✗ Connection failed: {e}")
            sys.exit(1)
    
    def ensure_connection(self):
        """Ensure database connection is alive, reconnect if needed"""
        try:
            # Simple query to check connection
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
        except (psycopg.OperationalError, psycopg.InterfaceError, AttributeError):
            logger.warning("⚠️  Connection lost, reconnecting...")
            self.connect()
    
    def load_existing_data(self):
        """Load existing data from database"""
        with self.conn.cursor(row_factory=dict_row) as cur:
            # Load users
            cur.execute("""
                SELECT user_id, username, account_level, total_playtime_hours 
                FROM dim_user 
                ORDER BY created_at DESC
            """)
            self.users = cur.fetchall()
            
            # Load games
            cur.execute("SELECT game_id, game_name, game_category FROM dim_game")
            self.games = cur.fetchall()
            
            # Validate that games exist
            if not self.games:
                logger.error("❌ No games found in dim_game table!")
                logger.error("Please run the seed data SQL to insert sample games.")
                sys.exit(1)
            
            # Load active sessions
            cur.execute("""
                SELECT session_id, user_id, game_id 
                FROM dim_session 
                WHERE is_active = TRUE
            """)
            for row in cur:
                self.active_sessions[row['user_id']] = {
                    'session_id': row['session_id'],
                    'game_id': row['game_id']
                }
        
        logger.info(f"✓ Loaded: {len(self.users)} users, {len(self.games)} games, {len(self.active_sessions)} active sessions")
    
    def create_user(self):
        """Create a new user using Faker"""
        # Generate realistic user data with Faker
        # Add timestamp to ensure uniqueness
        username = f"{fake.user_name()}{random.randint(100, 9999)}_{int(time.time())}"
        email = fake.email()
        country = fake.country()[:50]  # Limit to 50 chars
        
        try:
            with self.conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
                    INSERT INTO dim_user (username, email, country, account_level)
                    VALUES (%s, %s, %s, %s)
                    RETURNING user_id, username, account_level, total_playtime_hours
                """, (username, email, country, 'bronze'))
                
                user = cur.fetchone()
                self.conn.commit()
                
                self.users.append(user)
                self.stats['users_created'] += 1
                self.stats['total_operations'] += 1
                
                logger.info(f"→ New user: {username} from {country}")
                return user['user_id']
        except psycopg.errors.UniqueViolation:
            self.conn.rollback()
            logger.warning(f"⚠️  Username collision for {username}, skipping...")
            return None
    
    def update_user(self):
        """Update existing user (triggers CDC for SCD2)"""
        if not self.users:
            return
        
        user = random.choice(self.users)
        updates_made = []
        
        try:
            with self.conn.cursor() as cur:
                # Randomly choose update type: playtime OR account level
                update_type = random.random()
                
                if update_type < 0.7:  # 70% chance - update playtime
                    hours_played = Decimal(str(round(random.uniform(0.5, 3.0), 2)))
                    cur.execute("""
                        UPDATE dim_user 
                        SET total_playtime_hours = total_playtime_hours + %s,
                            last_login_at = NOW(),
                            updated_at = NOW()
                        WHERE user_id = %s
                    """, (hours_played, user['user_id']))
                    updates_made.append(f"+{hours_played}hrs")
                    user['total_playtime_hours'] = user.get('total_playtime_hours', Decimal('0')) + hours_played
                
                else:  # 30% chance - level progression
                    try:
                        current_level = user.get('account_level', 'bronze')
                        current_level_idx = self.account_levels.index(current_level)
                        if current_level_idx < len(self.account_levels) - 1:
                            new_level = self.account_levels[current_level_idx + 1]
                            cur.execute("""
                                UPDATE dim_user 
                                SET account_level = %s, updated_at = NOW()
                                WHERE user_id = %s
                            """, (new_level, user['user_id']))
                            updates_made.append(f"level→{new_level}")
                            user['account_level'] = new_level
                        else:
                            # Already max level, update playtime instead
                            hours_played = Decimal(str(round(random.uniform(0.5, 3.0), 2)))
                            cur.execute("""
                                UPDATE dim_user 
                                SET total_playtime_hours = total_playtime_hours + %s,
                                    last_login_at = NOW(),
                                    updated_at = NOW()
                                WHERE user_id = %s
                            """, (hours_played, user['user_id']))
                            updates_made.append(f"+{hours_played}hrs (max level)")
                    except ValueError:
                        # Invalid account level, reset to bronze and update playtime
                        logger.warning(f"⚠️  Invalid account_level '{current_level}' for user {user.get('username')}, resetting to bronze")
                        hours_played = Decimal(str(round(random.uniform(0.5, 3.0), 2)))
                        cur.execute("""
                            UPDATE dim_user 
                            SET account_level = 'bronze',
                                total_playtime_hours = total_playtime_hours + %s,
                                last_login_at = NOW(),
                                updated_at = NOW()
                            WHERE user_id = %s
                        """, (hours_played, user['user_id']))
                        updates_made.append(f"reset→bronze, +{hours_played}hrs")
                        user['account_level'] = 'bronze'
                
                self.conn.commit()
                self.stats['users_updated'] += 1
                self.stats['total_operations'] += 1
                logger.info(f"→ Updated {user['username']}: {', '.join(updates_made)}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Error updating user: {e}")
    
    def start_session(self):
        """Start a new gaming session"""
        if not self.users or not self.games:
            return
        
        # Find users without active sessions
        available_users = [u for u in self.users if u['user_id'] not in self.active_sessions]
        if not available_users:
            return
        
        user = random.choice(available_users)
        game = random.choice(self.games)
        device = random.choice(self.device_types)
        
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO dim_session (user_id, game_id, device_type, session_start_at)
                VALUES (%s, %s, %s, NOW())
                RETURNING session_id
            """, (user['user_id'], game['game_id'], device))
            
            session_id = cur.fetchone()[0]
            self.conn.commit()
            
            self.active_sessions[user['user_id']] = {
                'session_id': session_id,
                'game_id': game['game_id']
            }
            
            self.stats['sessions_started'] += 1
            self.stats['total_operations'] += 1
            
            logger.info(f"→ {user['username']} started playing {game['game_name']} on {device}")
            
            # Generate initial events
            self.generate_events(user['user_id'], session_id, game['game_id'], initial=True)
    
    def end_session(self):
        """End an active gaming session"""
        if not self.active_sessions:
            return
        
        user_id = random.choice(list(self.active_sessions.keys()))
        session_info = self.active_sessions[user_id]
        
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE dim_session 
                SET session_end_at = NOW(),
                    is_active = FALSE,
                    updated_at = NOW()
                WHERE session_id = %s
            """, (session_info['session_id'],))
            
            self.conn.commit()
            
            del self.active_sessions[user_id]
            self.stats['sessions_ended'] += 1
            self.stats['total_operations'] += 1
            
            # Find username for logging
            user = next((u for u in self.users if u['user_id'] == user_id), None)
            if user:
                logger.info(f"→ {user['username']} ended session")
    
    def generate_events(self, user_id, session_id, game_id, initial=False):
        """Generate game events for a session"""
        num_events = random.randint(1, 3) if initial else random.randint(1, 5)
        
        with self.conn.cursor() as cur:
            for _ in range(num_events):
                event_type = random.choice(self.event_types)
                
                # Generate realistic event data based on type
                score_delta = 0
                coins_earned = 0
                level_achieved = None
                metadata = {}
                
                if event_type == 'kill':
                    score_delta = random.randint(50, 200)
                    coins_earned = random.randint(5, 25)
                    metadata = {
                        'weapon': fake.word(),
                        'distance': random.randint(5, 100),
                        'headshot': random.choice([True, False])
                    }
                elif event_type == 'death':
                    score_delta = -random.randint(10, 50)
                    metadata = {
                        'killed_by': fake.user_name(),
                        'respawn_time': random.randint(3, 10)
                    }
                elif event_type == 'level_complete':
                    level_achieved = random.randint(1, 50)
                    score_delta = random.randint(500, 2000)
                    coins_earned = random.randint(50, 200)
                    metadata = {
                        'completion_time': random.randint(60, 1800),
                        'stars_earned': random.randint(1, 3)
                    }
                elif event_type == 'achievement_unlocked':
                    score_delta = random.randint(100, 500)
                    coins_earned = random.randint(25, 100)
                    metadata = {
                        'achievement_name': fake.catch_phrase(),
                        'rarity': random.choice(['common', 'rare', 'epic', 'legendary'])
                    }
                elif event_type == 'item_collected':
                    coins_earned = random.randint(1, 50)
                    metadata = {
                        'item_name': fake.word(),
                        'item_type': random.choice(['weapon', 'armor', 'consumable', 'currency'])
                    }
                else:
                    score_delta = random.randint(10, 100)
                    coins_earned = random.randint(0, 20)
                
                # Insert event
                cur.execute("""
                    INSERT INTO fact_game_event 
                    (session_id, user_id, game_id, event_type, score_delta, 
                     coins_earned, level_achieved, metadata, event_timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """, (
                    session_id, user_id, game_id, event_type, 
                    score_delta, coins_earned, level_achieved,
                    json.dumps(metadata) if metadata else None
                ))
                
                self.stats['events_created'] += 1
            
            self.conn.commit()
            self.stats['total_operations'] += num_events
    
    def generate_activity_for_active_sessions(self):
        """Generate events for active sessions"""
        if not self.active_sessions:
            return
        
        # Pick random active sessions (up to 3)
        num_sessions = min(3, len(self.active_sessions))
        selected_users = random.sample(list(self.active_sessions.keys()), num_sessions)
        
        for user_id in selected_users:
            session_info = self.active_sessions[user_id]
            self.generate_events(user_id, session_info['session_id'], session_info['game_id'])
    
    def print_stats(self):
        """Print simulation statistics"""
        logger.info("="*60)
        logger.info("📊 SIMULATION STATISTICS")
        logger.info(f"   Users created: {self.stats['users_created']}")
        logger.info(f"   Users updated: {self.stats['users_updated']} (CDC triggers)")
        logger.info(f"   Sessions: {self.stats['sessions_started']} started, {self.stats['sessions_ended']} ended")
        logger.info(f"   Events created: {self.stats['events_created']}")
        logger.info(f"   Total operations: {self.stats['total_operations']}")
        logger.info(f"   Active sessions: {len(self.active_sessions)}")
        logger.info("="*60)
    
    def run(self):
        """Main simulation loop"""
        print("\n" + "="*60)
        print("🎮 Gaming Data Simulator for CDC Testing")
        print(f"📍 Database: {self.db_config['host']}")
        print(f"📊 Stats interval: {self.stats_interval} seconds")
        print("="*60)
        print("Press Ctrl+C to stop\n")
        
        # Connect to database
        self.connect()
        
        last_stats_time = time.time()
        
        try:
            while True:
                # Ensure connection is alive (check every cycle)
                self.ensure_connection()
                
                # Select action based on weighted probabilities
                action_rand = random.random()
                cumulative = 0
                
                # Build weighted action list
                cumulative += self.probabilities['new_user']
                if action_rand < cumulative:
                    self.create_user()
                    
                elif action_rand < (cumulative := cumulative + self.probabilities['update_user']):
                    self.update_user()
                    
                elif action_rand < (cumulative := cumulative + self.probabilities['start_session']):
                    self.start_session()
                    
                elif action_rand < (cumulative := cumulative + self.probabilities['end_session']):
                    self.end_session()
                    
                elif action_rand < (cumulative := cumulative + self.probabilities['generate_events']):
                    self.generate_activity_for_active_sessions()
                
                # Print stats at configured interval
                if time.time() - last_stats_time > self.stats_interval:
                    self.print_stats()
                    last_stats_time = time.time()
                
                # Delay between cycles
                time.sleep(random.uniform(self.cycle_delay * 0.5, self.cycle_delay * 1.5))
                
        except KeyboardInterrupt:
            print("\n\n⚠️  Keyboard interrupt received, shutting down...")
            
        except Exception as e:
            logger.error(f"❌ Error in simulation: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            # Cleanup
            if self.conn:
                self.conn.close()
                logger.info("✓ Database connection closed")
            
            print("\n" + "="*60)
            self.print_stats()
            print("="*60)
            print("✓ Simulator stopped successfully\n")

def main():
    """Main entry point"""
    # Check if data-simulator.env exists
    if not ENV_FILE.exists():
        logger.error("data-simulator.env file not found! Please create it with database configuration.")
        print("\nExample data-simulator.env file:")
        print("DB_HOST=your-rds-endpoint.amazonaws.com")
        print("DB_PASSWORD=your_password")
        print("DB_USER=postgres")
        print("DB_NAME=postgres")
        sys.exit(1)
    
    # Create and run simulator
    simulator = GamingDataSimulator()
    simulator.run()

if __name__ == "__main__":
    main()
