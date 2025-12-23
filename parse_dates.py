"""
PySpark Date Parser Module

This module provides utilities for parsing dates in multiple formats including:
- Brazilian format (DD/MM/YYYY)
- American format (MM/DD/YYYY)
- ISO format (YYYY-MM-DD)
- Written-out formats (e.g., "01 de feb. de 2024", "01 de fevereiro de 2024")
- Other common formats

Author: mateussantiagoaon
"""

from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from datetime import datetime
import re


def parse_dates(df, column_name: str, output_column_name: str = None, assumed_format: str = "brazilian"):
    """
    Parse dates in multiple formats from a DataFrame column.
    
    This function attempts to parse dates in various formats and returns a new DataFrame
    with a DateType column containing the parsed dates.
    
    Parameters:
    -----------
    df : pyspark.sql.DataFrame
        The input DataFrame containing the date column to parse
    column_name : str
        The name of the column containing dates to parse
    output_column_name : str, optional
        The name of the output column (defaults to column_name + "_parsed")
    assumed_format : str, optional
        The assumed format for ambiguous dates. Options: "brazilian" (DD/MM/YYYY) or "american" (MM/DD/YYYY)
        Default: "brazilian"
    
    Returns:
    --------
    pyspark.sql.DataFrame
        DataFrame with a new column containing parsed dates as DateType
    
    Examples:
    ---------
    >>> df = spark.createDataFrame([("01/02/2024",), ("2024-02-01",), ("01 de fev. de 2024",)], ["date_str"])
    >>> parsed_df = parse_dates(df, "date_str")
    >>> parsed_df.show()
    """
    
    if output_column_name is None:
        output_column_name = f"{column_name}_parsed"
    
    # Define month mappings for Portuguese written-out dates
    month_mappings = {
        "jan": "01", "janeiro": "01", "january": "01",
        "fev": "02", "fevereiro": "02", "february": "02",
        "mar": "03", "março": "03", "march": "03",
        "abr": "04", "abril": "04", "april": "04",
        "mai": "05", "maio": "05", "may": "05",
        "jun": "06", "junho": "06", "june": "06",
        "jul": "07", "julho": "07", "july": "07",
        "ago": "08", "agosto": "08", "august": "08",
        "set": "09", "setembro": "09", "september": "09",
        "out": "10", "outubro": "10", "october": "10",
        "nov": "11", "novembro": "11", "november": "11",
        "dez": "12", "dezembro": "12", "december": "12"
    }
    
    def parse_date_string(date_str):
        """
        Parse a date string in multiple formats.
        
        Supported formats:
        - DD/MM/YYYY (Brazilian)
        - MM/DD/YYYY (American)
        - YYYY-MM-DD (ISO)
        - DD-MM-YYYY
        - DD.MM.YYYY
        - DD de [month name] de YYYY (e.g., "01 de fevereiro de 2024")
        - DD [month name] YYYY (e.g., "01 fevereiro 2024")
        - [month name] DD, YYYY (e.g., "February 01, 2024")
        """
        
        if date_str is None or date_str.strip() == "":
            return None
        
        date_str = str(date_str).strip()
        
        try:
            # Try ISO format first (YYYY-MM-DD)
            try:
                return datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                pass
            
            # Try common Brazilian format (DD/MM/YYYY)
            try:
                return datetime.strptime(date_str, "%d/%m/%Y").date()
            except ValueError:
                pass
            
            # Try American format (MM/DD/YYYY)
            try:
                return datetime.strptime(date_str, "%m/%d/%Y").date()
            except ValueError:
                pass
            
            # Try DD-MM-YYYY
            try:
                return datetime.strptime(date_str, "%d-%m-%Y").date()
            except ValueError:
                pass
            
            # Try DD.MM.YYYY
            try:
                return datetime.strptime(date_str, "%d.%m.%Y").date()
            except ValueError:
                pass
            
            # Try written-out format: "DD de [month] de YYYY" or "DD de [month] YYYY"
            written_out_pattern = r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})"
            match = re.match(written_out_pattern, date_str, re.IGNORECASE)
            if match:
                day, month_str, year = match.groups()
                month_lower = month_str.lower().rstrip(".")
                if month_lower in month_mappings:
                    month = month_mappings[month_lower]
                    date_str_normalized = f"{day}/{month}/{year}"
                    return datetime.strptime(date_str_normalized, "%d/%m/%Y").date()
            
            # Try format: "DD [month] YYYY" or "DD [month] YYYY"
            alt_written_out_pattern = r"(\d{1,2})\s+(\w+)\s+(\d{4})"
            match = re.match(alt_written_out_pattern, date_str, re.IGNORECASE)
            if match:
                day, month_str, year = match.groups()
                month_lower = month_str.lower().rstrip(".")
                if month_lower in month_mappings:
                    month = month_mappings[month_lower]
                    date_str_normalized = f"{day}/{month}/{year}"
                    return datetime.strptime(date_str_normalized, "%d/%m/%Y").date()
            
            # Try American written format: "[month] DD, YYYY"
            american_pattern = r"(\w+)\s+(\d{1,2}),?\s+(\d{4})"
            match = re.match(american_pattern, date_str, re.IGNORECASE)
            if match:
                month_str, day, year = match.groups()
                month_lower = month_str.lower().rstrip(".")
                if month_lower in month_mappings:
                    month = month_mappings[month_lower]
                    date_str_normalized = f"{day}/{month}/{year}"
                    return datetime.strptime(date_str_normalized, "%d/%m/%Y").date()
            
            # If we get here, unable to parse
            return None
            
        except Exception as e:
            # Log parsing errors silently and return None
            return None
    
    # Create a Spark UDF from the parsing function
    parse_date_udf = F.udf(parse_date_string, DateType())
    
    # Apply the UDF to the DataFrame
    df_parsed = df.withColumn(output_column_name, parse_date_udf(F.col(column_name)))
    
    return df_parsed


def parse_dates_with_format(df, column_name: str, date_format: str, output_column_name: str = None):
    """
    Parse dates using a specific format string.
    
    This function is useful when you know the exact format of the dates in advance.
    
    Parameters:
    -----------
    df : pyspark.sql.DataFrame
        The input DataFrame containing the date column to parse
    column_name : str
        The name of the column containing dates to parse
    date_format : str
        The date format string (e.g., "dd/MM/yyyy", "MM/dd/yyyy")
    output_column_name : str, optional
        The name of the output column (defaults to column_name + "_parsed")
    
    Returns:
    --------
    pyspark.sql.DataFrame
        DataFrame with a new column containing parsed dates as DateType
    
    Examples:
    ---------
    >>> df = spark.createDataFrame([("01/02/2024",), ("15/03/2024",)], ["date_str"])
    >>> parsed_df = parse_dates_with_format(df, "date_str", "dd/MM/yyyy")
    >>> parsed_df.show()
    """
    
    if output_column_name is None:
        output_column_name = f"{column_name}_parsed"
    
    df_parsed = df.withColumn(
        output_column_name,
        F.to_date(F.col(column_name), date_format)
    )
    
    return df_parsed


if __name__ == "__main__":
    # Example usage (requires PySpark to be installed)
    from pyspark.sql import SparkSession
    
    # Create a Spark session
    spark = SparkSession.builder.appName("DateParser").getOrCreate()
    
    # Sample data with various date formats
    sample_data = [
        ("01/02/2024",),
        ("02/01/2024",),
        ("2024-02-01",),
        ("01 de fevereiro de 2024",),
        ("01 de fev. de 2024",),
        ("01 febrero 2024",),
        ("February 01, 2024",),
        ("01.02.2024",),
        ("01-02-2024",),
        (None,),
    ]
    
    # Create DataFrame
    df = spark.createDataFrame(sample_data, ["date_string"])
    
    print("Original DataFrame:")
    df.show()
    
    # Parse dates
    df_parsed = parse_dates(df, "date_string", assumed_format="brazilian")
    
    print("\nParsed DataFrame:")
    df_parsed.show()
    
    # Alternative: use a specific format
    df_specific = parse_dates_with_format(df, "date_string", "dd/MM/yyyy")
    print("\nParsed with specific format (dd/MM/yyyy):")
    df_specific.show()
