from data_util import Portfolio
import pandas as pd
import os



if __name__ == "__main__":

    try:

        # Pyspark transforms to answer questions...
        stock_df = pd.read_csv("data/stocks.csv")

        portfolio1 = Portfolio(
            "portfolio1", stock_df["symbol"].tolist(), "2023-01-01", "2023-12-31"
        )

        # Run data export method for portfolio instance if data export is required
        #Set the polygon API key here if you want to run the export (should be env var in production)
        #portfolio1.export_data("WPrrrs77Czp84b48wFp75_toF_gaa0On")
        
        print("Question 1 - Which stock has had the greatest relative increase in price in this period?")
        portfolio1.question1_pipeline()

        print("Question 2 - If you had invested $1 million at the beginning of this period by purchasing $10,000 worth of shares in every company in the list equally, how much would you have today? Technical note, you can assume that it is possible to purchase fractional shares?")
        portfolio1.question2_pipeline()
        
        print("Question 3 - Which stock had the greatest value in monthly CAGR between January and June?")
        print("NOTE: Assumed that 'monthly CAGR' is the CMGR between January and June ref- https://www.wallstreetprep.com/knowledge/compound-monthly-growth-rate-cmgr/")
        portfolio1.question3_pipeline()

        # 4. During the year, which stock had the greatest decrease in value within a single week and which week was this?
        print("Question 4 - During the year, which stock had the greatest decrease in value within a single week and which week was this?")
        portfolio1.question4_pipeline()

    except Exception as e:
        print(f"Error: {e}")
