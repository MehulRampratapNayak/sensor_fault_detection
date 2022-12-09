from sensor.entity import artifact_entity,config_entity
from sensor.exception import SensorException
from sensor.logger import logging
from typing import Optional
import os,sys
from sklearn.pipeline import Pipeline
import pandas as pd
from sensor import utils
import numpy as np
from sklearn.preprocessing import LabelEncoder
from imblearn.combine import SMOTETomek
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import RobustScaler
from sensor.config import TARGET_COLUMN


class DataTransformation:

    def __init__(self,data_transformation_config:config_entity.DataTransformationConfig,
                    data_ingestion_artifact:artifact_entity.DataIngestionArtifact):
        
        try:
            logging.info(f"{'>>'*20} Data Transformaton {'<<'*20}")
            self.data_transformation_config = data_transformation_config
            self.data_ingestion_artifact = data_ingestion_artifact
        except Exception as e:
            raise SensorException(e,sys)
            

    @classmethod
    def get_data_transformer_object(cls):
        try:
            simple_imputer=SimpleImputer(strategy='constant',fill_value=0)
            robust_scaler=RobustScaler()
            pipeline=Pipeline(steps=[
                ('Imputer',simple_imputer),
                ('RobustScaler',robust_scaler)
            ])
            return pipeline
        except Exception as e:
            raise SensorException(e, sys)
