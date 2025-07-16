#!/usr/bin/env python3
"""
Cost-optimized NAIP data acquisition script for Dallas flood risk analysis
Downloads NAIP imagery for specified zip code and transfers to S3

Usage: python download_naip_dallas.py <zip_code>
Example: python download_naip_dallas.py 75287
"""

import boto3
import rasterio
from rasterio.session import AWSSession
import os
import sys
import time
import logging
from datetime import datetime
import json
from pathlib import Path
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NAIPDownloader:
    def __init__(self, target_bucket, target_prefix):
        """
        Initialize downloader with minimal cost configuration
        
        Args:
            target_bucket: Your S3 bucket name (dallas-flood-raw-data)
            target_prefix: S3 prefix for organizing data (imagery/naip)
        """
        self.target_bucket = target_bucket
        self.target_prefix = target_prefix
        
        # Load zip code mapping configuration
        self.load_zip_mapping()
        
        # Configure AWS session for requester-pays
        os.environ['AWS_REQUEST_PAYER'] = 'requester'
        self.session = boto3.Session()
        self.s3_client = self.session.client('s3', region_name='us-east-1')
        self.s3_source_client = self.session.client('s3', region_name='us-west-2')
        self.aws_session = AWSSession(self.session, requester_pays=True)
        
        # Cost optimization: Single thread, no acceleration
        self.config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=1024 * 25,  # 25MB
            max_concurrency=1,  # Single thread to minimize cost
            multipart_chunksize=1024 * 25,
            use_threads=False  # Disable threading
        )
    
    def load_zip_mapping(self):
        """Load zip code to quad ID mapping from configuration file"""
        config_path = Path(__file__).parent / 'dallas_zip_quad_mapping.json'
        
        if config_path.exists():
            with open(config_path, 'r') as f:
                self.zip_config = json.load(f)
                logger.info(f"Loaded zip code mapping for {len(self.zip_config['zip_code_mapping'])} zip codes")
        else:
            logger.warning("Zip code mapping file not found, using fallback geocoding")
            self.zip_config = None
    
    def get_zip_code_coordinates(self, zip_code):
        """
        Get coordinates for a zip code using free geocoding service
        Returns (lat, lon) tuple or None if not found
        """
        try:
            # Using US Census Geocoding API (free, no key required)
            url = f"https://geocoding.geo.census.gov/geocoder/locations/address"
            params = {
                'zip': zip_code,
                'benchmark': 'Public_AR_Current',
                'format': 'json'
            }
            
            response = requests.get(url, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if data.get('result') and data['result'].get('addressMatches'):
                    match = data['result']['addressMatches'][0]
                    coords = match['coordinates']
                    return (coords['y'], coords['x'])  # lat, lon
            
            # Fallback: hardcoded coordinates for common Dallas zip codes
            dallas_zip_coords = {
                '75287': (33.0005, -96.8314),
                '75201': (32.7831, -96.8067),
                '75202': (32.7806, -96.7970),
                '75203': (32.7487, -96.7919),
                '75204': (32.8029, -96.7828),
                # Add more as needed
            }
            
            if zip_code in dallas_zip_coords:
                return dallas_zip_coords[zip_code]
                
        except Exception as e:
            logger.warning(f"Could not geocode zip {zip_code}: {str(e)}")
        
        return None
    
    def get_quad_ids_for_location(self, lat, lon):
        """
        Determine USGS quarter quad IDs for a given lat/lon
        Returns empty list - actual quads must be determined by searching S3
        or using verified mappings
        """
        logger.warning(f"No verified quad IDs for coordinates {lat:.4f}, {lon:.4f}")
        logger.warning("Run find_naip_quads.py to discover actual quad IDs for this location")
        
        # Return empty list to avoid generating invalid quad IDs
        # The actual quad IDs must come from the configuration file
        # or be discovered using the find_naip_quads.py script
        return []
    
    def get_dallas_tiles(self, zip_code):
        """
        Define NAIP tiles covering the specified Dallas zip code
        
        Args:
            zip_code: 5-digit zip code string
            
        Returns:
            List of tile dictionaries with download information
        """
        logger.info(f"Looking up tiles for zip code {zip_code}")
        
        # First try to use configured mapping
        if self.zip_config and zip_code in self.zip_config['zip_code_mapping']:
            zip_info = self.zip_config['zip_code_mapping'][zip_code]
            
            if zip_info.get('quad_ids') and zip_info['verified']:
                logger.info(f"Using verified quad IDs for {zip_info['name']}")
                quad_ids = zip_info['quad_ids']
            else:
                # Use coordinates to calculate quads
                lat = zip_info['coordinates']['lat']
                lon = zip_info['coordinates']['lon']
                logger.info(f"Using coordinates for {zip_info['name']}: {lat:.4f}, {lon:.4f}")
                quad_ids = self.get_quad_ids_for_location(lat, lon)
                
                if not zip_info.get('quad_ids'):
                    logger.warning(f"No verified quad IDs for {zip_code}. Using calculated: {quad_ids}")
        else:
            # Fall back to geocoding
            coords = self.get_zip_code_coordinates(zip_code)
            if not coords:
                raise ValueError(f"Could not find coordinates for zip code {zip_code}")
            
            lat, lon = coords
            logger.info(f"Geocoded zip code {zip_code}: {lat:.4f}, {lon:.4f}")
            quad_ids = self.get_quad_ids_for_location(lat, lon)
        
        logger.info(f"Using {len(quad_ids)} quad IDs: {quad_ids}")
        
        # Get years from config or use defaults
        years = self.zip_config.get('naip_years', [2020, 2022, 2024]) if self.zip_config else [2020, 2022, 2024]
        
        tiles = []
        for year in years:
            for quad_id in quad_ids:
                # NAIP naming convention
                for quadrant in ['nw', 'ne', 'sw', 'se']:
                    tile_name = f'm_{quad_id}_{quadrant}_14_060_{year}0815.tif'
                    s3_path = f's3://naip-analytic/tx/{year}/60cm/rgbir_cog/{quad_id[:5]}/{tile_name}'
                    tiles.append({
                        'year': year,
                        'quad_id': quad_id,
                        'quadrant': quadrant,
                        's3_path': s3_path,
                        'filename': tile_name,
                        'zip_code': zip_code
                    })
        
        return tiles
    
    def check_tile_exists(self, s3_path):
        """Check if a tile exists in NAIP bucket before attempting download"""
        bucket = s3_path.split('/')[2]
        key = '/'.join(s3_path.split('/')[3:])
        
        try:
            self.s3_source_client.head_object(
                Bucket=bucket,
                Key=key,
                RequestPayer='requester'
            )
            return True
        except:
            return False
    
    def copy_tile_to_s3(self, tile_info):
        """
        Copy a single tile from NAIP to your S3 bucket
        Uses S3 copy to minimize data transfer costs
        """
        source_bucket = tile_info['s3_path'].split('/')[2]
        source_key = '/'.join(tile_info['s3_path'].split('/')[3:])
        
        # Organize by year in target bucket
        target_key = f"{self.target_prefix}/{tile_info['year']}/{tile_info['filename']}"
        
        try:
            # First check if already exists in target
            try:
                self.s3_client.head_object(Bucket=self.target_bucket, Key=target_key)
                logger.info(f"Already exists: {target_key}")
                return True
            except:
                pass  # File doesn't exist, proceed with copy
            
            # Check if source exists
            if not self.check_tile_exists(tile_info['s3_path']):
                logger.warning(f"Source not found: {tile_info['s3_path']}")
                return False
            
            # Copy directly between S3 buckets (most cost-effective)
            copy_source = {
                'Bucket': source_bucket,
                'Key': source_key,
                'RequestPayer': 'requester'
            }
            
            logger.info(f"Copying {source_key} to {target_key}")
            
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=self.target_bucket,
                Key=target_key,
                RequestPayer='requester',
                MetadataDirective='COPY'
            )
            
            # Add metadata about the transfer
            self.s3_client.put_object_tagging(
                Bucket=self.target_bucket,
                Key=target_key,
                Tagging={
                    'TagSet': [
                        {'Key': 'Source', 'Value': 'NAIP'},
                        {'Key': 'Year', 'Value': str(tile_info['year'])},
                        {'Key': 'QuadID', 'Value': tile_info['quad_id']},
                        {'Key': 'ZipCode', 'Value': tile_info['zip_code']},
                        {'Key': 'TransferDate', 'Value': datetime.now().isoformat()}
                    ]
                }
            )
            
            logger.info(f"Successfully copied: {target_key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to copy {tile_info['filename']}: {str(e)}")
            return False
    
    def download_with_rasterio_fallback(self, tile_info):
        """
        Fallback method using rasterio if S3 copy fails
        This is slower but more reliable for complex cases
        """
        import tempfile
        
        source_path = tile_info['s3_path']
        target_key = f"{self.target_prefix}/{tile_info['year']}/{tile_info['filename']}"
        
        try:
            with rasterio.Env(aws_session=self.aws_session):
                with rasterio.open(source_path) as src:
                    profile = src.profile
                    data = src.read()
                    
                    logger.info(f"Downloaded to memory: {tile_info['filename']}")
                    logger.info(f"Image shape: {data.shape}, dtype: {data.dtype}")
                    
                    # Write to temporary file
                    with tempfile.NamedTemporaryFile(suffix='.tif', delete=False) as tmp:
                        tmp_path = tmp.name
                        
                    with rasterio.open(tmp_path, 'w', **profile) as dst:
                        dst.write(data)
                    
                    # Upload to S3
                    self.s3_client.upload_file(
                        tmp_path,
                        self.target_bucket,
                        target_key,
                        Config=self.config
                    )
                    
                    # Clean up
                    os.unlink(tmp_path)
                    
                    logger.info(f"Uploaded via rasterio: {target_key}")
                    return True
                    
        except Exception as e:
            logger.error(f"Rasterio fallback failed for {tile_info['filename']}: {str(e)}")
            return False
    
    def process_all_tiles(self, zip_code):
        """Process all tiles for a given zip code with cost optimization"""
        tiles = self.get_dallas_tiles(zip_code)
        logger.info(f"Processing {len(tiles)} potential tiles for Dallas {zip_code}")
        
        results = {
            'successful': [],
            'failed': [],
            'skipped': []
        }
        
        for i, tile in enumerate(tiles):
            logger.info(f"Processing {i+1}/{len(tiles)}: {tile['filename']}")
            
            # Try S3 copy first (most cost-effective)
            if self.copy_tile_to_s3(tile):
                results['successful'].append(tile['filename'])
            else:
                # Try rasterio fallback
                logger.info(f"Attempting rasterio fallback for {tile['filename']}")
                if self.download_with_rasterio_fallback(tile):
                    results['successful'].append(tile['filename'])
                else:
                    results['failed'].append(tile['filename'])
            
            # Brief pause to avoid rate limiting
            time.sleep(0.5)
        
        # Save results summary with zip code
        summary = {
            'timestamp': datetime.now().isoformat(),
            'zip_code': zip_code,
            'total_tiles': len(tiles),
            'successful': len(results['successful']),
            'failed': len(results['failed']),
            'results': results
        }
        
        # Upload summary to S3
        summary_key = f"{self.target_prefix}/download_summary_{zip_code}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        self.s3_client.put_object(
            Bucket=self.target_bucket,
            Key=summary_key,
            Body=json.dumps(summary, indent=2),
            ContentType='application/json'
        )
        
        logger.info(f"Download complete. Summary uploaded to {summary_key}")
        return results
    
    def verify_downloads(self):
        """Verify all expected files are in the target bucket"""
        logger.info("Verifying downloads...")
        
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(
            Bucket=self.target_bucket,
            Prefix=f"{self.target_prefix}/"
        )
        
        files_by_year = {}
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key_parts = obj['Key'].split('/')
                    if len(key_parts) >= 3 and key_parts[2].isdigit():
                        year = key_parts[2]
                        if year not in files_by_year:
                            files_by_year[year] = []
                        files_by_year[year].append(obj['Key'])
        
        for year, files in sorted(files_by_year.items()):
            logger.info(f"Year {year}: {len(files)} files")
        
        return files_by_year


def main():
    """Main execution function"""
    # Check command line arguments
    if len(sys.argv) != 2:
        print("Usage: python download_naip_dallas.py <zip_code>")
        print("Example: python download_naip_dallas.py 75287")
        sys.exit(1)
    
    zip_code = sys.argv[1]
    
    # Validate zip code format
    if not zip_code.isdigit() or len(zip_code) != 5:
        print(f"Error: '{zip_code}' is not a valid 5-digit zip code")
        sys.exit(1)
    
    # Configuration
    TARGET_BUCKET = 'dallas-flood-raw-data'
    TARGET_PREFIX = 'imagery/naip'
    
    logger.info(f"Starting NAIP data acquisition for Dallas zip code {zip_code}")
    logger.info(f"Target: s3://{TARGET_BUCKET}/{TARGET_PREFIX}")
    
    # Initialize downloader
    downloader = NAIPDownloader(TARGET_BUCKET, TARGET_PREFIX)
    
    try:
        # Process all tiles
        results = downloader.process_all_tiles(zip_code)
        
        # Verify downloads
        verified_files = downloader.verify_downloads()
        
        logger.info("NAIP data acquisition complete!")
        logger.info(f"Successfully downloaded: {len(results['successful'])} tiles")
        logger.info(f"Failed: {len(results['failed'])} tiles")
        
        if results['failed']:
            logger.warning("Failed tiles:")
            for tile in results['failed']:
                logger.warning(f"  - {tile}")
                
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
