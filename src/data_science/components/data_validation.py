import pandas as pd
from src.data_science.entity.config_entity import DataValidationConfig
from src.data_science import logger

## Component - Data Validation

class DataValiadtion:
    def __init__(self, config: DataValidationConfig):
        self.config = config

    def validate_all_columns_and_dtypes(self)-> bool:
        try:

            data = pd.read_csv(self.config.unzip_data_dir)
            # Extract actual column names and dtypes from the DataFrame
            all_cols = list(data.columns)
            all_dtypes = [col_dtype.name for col_dtype in data.dtypes.values]

            # Extract schema column names and expected dtypes
            all_schema_name = self.config.all_schema.keys()
            all_schema_dtype = list(self.config.all_schema.values())

            # Check if all columns in the data match the schema columns (order doesn't matter)
            all_columns_match = set(all_cols) == set(all_schema_name)

            # Check if all datatypes match between schema and data (order matters)
            all_dtypes_match = (
                all(a == b for a, b in zip(all_schema_dtype, all_dtypes)) and
                len(all_schema_dtype) == len(all_dtypes)
            )

            # Final validation: both columns and dtypes must match
            validation_status = all_columns_match and all_dtypes_match

            # Write validation results to a status file
            with open(self.config.STATUS_FILE, 'w') as f:
                f.write(f"Validation status: {validation_status}\n")
                # If columns do not match, write details about missing/extra columns
                if not all_columns_match:
                    f.write(f"All columns match: {all_columns_match}\n")
                    missing = set(all_schema_name) - set(all_cols)
                    extra = set(all_cols) - set(all_schema_name)
                    f.write(f"Missing columns: {missing}\n")
                    f.write(f"Extra columns: {extra}\n")
                # If datatypes do not match, write details about mismatched columns
                if not all_dtypes_match:
                    f.write(f"All dtypes match: {all_dtypes_match}\n")
                    for i, (expected, actual) in enumerate(zip(all_schema_dtype, all_dtypes)):
                        if expected != actual:
                            f.write(f"Column '{all_cols[i]}' dtype mismatch: expected {expected}, got {actual}\n")
            return validation_status
        
        except Exception as e:
            raise e