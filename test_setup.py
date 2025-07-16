#!/usr/bin/env python3
"""Test script to verify installation"""

import sys
print("Testing package imports...")

packages = [
    'rasterio', 'geopandas', 'tensorflow',
    'boto3', 'numpy', 'pandas', 'sklearn'
]

failed = []
for package in packages:
    try:
        __import__(package)
        print(f"✓ {package}")
    except ImportError as e:
        print(f"✗ {package}: {e}")
        failed.append(package)

# Test GDAL separately since it's imported differently
try:
    from osgeo import gdal
    print(f"✓ GDAL")
except ImportError as e:
    print(f"✗ GDAL: {e}")
    failed.append('GDAL')

if failed:
    print(f"\nFailed to import: {', '.join(failed)}")
    sys.exit(1)
else:
    print("\nAll packages imported successfully!")
    
# Show versions
from osgeo import gdal
print(f"\nGDAL Version: {gdal.__version__}")

import tensorflow as tf
print(f"TensorFlow Version: {tf.__version__}")
print(f"TensorFlow GPU Available: {len(tf.config.list_physical_devices('GPU'))} GPUs")

# Test AWS credentials
import boto3
try:
    sts = boto3.client('sts')
    identity = sts.get_caller_identity()
    print(f"\nAWS Identity: {identity['Arn']}")
except Exception as e:
    print(f"\nAWS credentials not configured: {e}")
