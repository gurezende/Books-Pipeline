# Imports
import os

# Extract Data
import requests

# Transform Data
import pandas as pd

# Load Data
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String



class BooksPipeline:
    def __init__(self, category):
        self.category = category

    ##### EXTRACT DATA #####

    def extract_data(self):
        """
        Extracts book data for a given category from the Open Library API.

        Args:
            category (str): The category of books to extract.

        Returns:
            dict: A JSON response containing details about the books in the specified category.
        """
        
        # Set books category
        category = self.category

        # Get books
        url = f"https://openlibrary.org/subjects/{category}.json?details=true"

        # Get response
        return requests.get(url).json()
        

    ##### TRANSFORM DATA #####

    def transform_data(self, response):
        """
        Transforms the raw JSON data into a Pandas DataFrame, selects the
        relevant columns, renames them, and updates the 'link' column to include
        the full URL. It also updates the 'author' column by exploding each list
        of authors and taking only the name.

        Args:
            data (dict): The JSON response from the Open Library API.

        Returns:
            pd.DataFrame: The transformed data.
        """

        # Dataframe
        data = pd.DataFrame(response["works"])
        data = data[["title", "authors", "first_publish_year", "key"]]
        
        # Rename columns
        data.columns = ["title", "author", "published_year", "link"]

        # Add category column
        data["category"] = self.category

        # Reindex columns
        data = data.reindex(columns=["title", "category", "author", "published_year", "link"])

        # Update columns Link
        data["link"] = "https://openlibrary.org" + data["link"]

        # Update columns Author. 
        # Explode each list of authors and take only the first author name
        data["author"] = data["author"].explode().str["name"].groupby(level=0).first()

        return data
    
    
    ##### LOAD DATA #####

    def load_data(self, DB_HOST=os.getenv("DB_HOST"), DB_NAME=os.getenv("DB_NAME"), 
                  DB_USER=os.getenv("DB_USER"), DB_PASSWORD=os.getenv("DB_PASSWORD"), 
                  data=pd.DataFrame()):
    
        """
        Loads the transformed data into a PostgreSQL database.

        This method loads environment variables, creates a connection to a PostgreSQL
        database, creates a table named "books" if it doesn't exist, and inserts the
        transformed data into the table.

        Args:
            None

        Returns:
            None
        """

        # Update the DATABASE_URL with your actual PostgreSQL credentials
        DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"

        # Create the engine for postgresql
        engine = create_engine(DATABASE_URL)

        # Create the base class
        Base = declarative_base()

        # Creating a model for the books table
        class Books(Base):
            __tablename__ = "books"
            
            # Table columns
            id = Column(Integer, primary_key=True, index=True, autoincrement="auto")
            category = Column(String)
            title = Column(String)
            author = Column(String)
            published_year = Column(Integer)
            link = Column(String)

        # Create the table
        Base.metadata.create_all(engine)

        # Create the local session
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


        # Insert the data into the existing table
        data.to_sql(name="books",
                        con=engine, 
                        if_exists="append", 
                        index=False, 
                        method="multi")
        
        print(f"{data.shape[0]} observations successfully loaded into {DB_NAME} database")


# Run
if __name__ == "__main__":

    # Instance
    collector = BooksPipeline(category="fiction")
    
    # ETL
    response = collector.extract_data()
    data = collector.transform_data(response=response)
    collector.load_data(data=data)
