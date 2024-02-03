import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class Tables :
    def __init__(self,conn,cursor):

        self.conn = conn
        self.cursor = cursor
        self.create_tables()


    def create_tables(self):
        try:
            self.create_campaign_table()
            self.create_customers_table()
            self.create_product_table()
            self.create_events_table()
            self.create_campaign_product_interaction_table()
            self.conn.commit()

        except Exception as e:
            logger.exception(e)



    def create_product_table(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS
        public.product (
        id integer PRIMARY KEY,
        uuid_product uuid DEFAULT gen_random_uuid(),
        name varchar(13),
        description varchar(255),
        category varchar(20),
        picture varchar(500),
        added_at timestamp DEFAULT CURRENT_TIMESTAMP,
        valid_product boolean DEFAULT TRUE);
        """)

    def create_campaign_table(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS
        public.campaign(
        id integer PRIMARY KEY,
        uuid_campaign uuid DEFAULT gen_random_uuid(),
        name varchar(20),
        created_by varchar(200),
        description varchar(255),
        valid_campaign boolean DEFAULT TRUE,
        creation_date timestamp DEFAULT CURRENT_TIMESTAMP,
        tags VARCHAR(30)
        );""")

    def create_events_table(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS 
        public.events(
        id integer PRIMARY KEY,
        customer_id integer NOT NULL,
        campaign_id integer NOT NULL,
        type_event varchar(20),
        event_date timestamp DEFAULT CURRENT_TIMESTAMP,
        user_agent varchar(255)
        );""")

    def create_customers_table(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS
        public.customers(
        id integer PRIMARY KEY,
        name_customer varchar(20),
        description_customer varchar(255),
        creation_date timestamp DEFAULT CURRENT_TIMESTAMP,
        modified_date timestamp DEFAULT CURRENT_TIMESTAMP,
        valid_customer boolean DEFAULT TRUE
        );""")

    def create_campaign_product_interaction_table(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS 
        public.campaign_product_interactions(
        id integer PRIMARY KEY,
        product_id integer NOT NULL,
        campaign_id integer NOT NULL,
        linked_date  timestamp DEFAULT CURRENT_TIMESTAMP,
        linked_by varchar(200)
        );""")