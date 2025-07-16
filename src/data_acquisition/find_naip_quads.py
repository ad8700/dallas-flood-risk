#!/usr/bin/env python3
"""
NAIP Quad ID Finder - Discovers actual NAIP quad IDs by exploring S3
Searches systematically through the NAIP bucket to find available tiles
"""

import boto3
import sys
import json
import logging
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class NAIPQuadFinder:
    def __init__(self):
        self.s3_client = boto3.client('s3', region_name='us-west-2')
        self.found_quads = defaultdict(list)
        
    def explore_naip_structure(self, year=2022):
        """
        Explore the actual NAIP S3 structure to understand the quad numbering
        """
        logger.info(f"Exploring NAIP structure for year {year}...")
        
        try:
            # What prefixes exist at the quad level
            response = self.s3_client.list_objects_v2(
                Bucket='naip-analytic',
                Prefix=f'tx/{year}/60cm/rgbir_cog/',
                Delimiter='/',
                MaxKeys=1000,
                RequestPayer='requester'
            )
            
            prefixes = []
            if 'CommonPrefixes' in response:
                for prefix in response['CommonPrefixes']:
                    quad_prefix = prefix['Prefix'].split('/')[-2]
                    prefixes.append(quad_prefix)
                    
            logger.info(f"Found {len(prefixes)} quad prefixes")
            logger.info(f"Sample prefixes: {prefixes[:10]}")
            
            return sorted(prefixes)
            
        except Exception as e:
            logger.error(f"Error exploring structure: {e}")
            return []
    
    def find_dallas_area_quads(self, year=2022):
        """
        Search for Dallas-area quads by looking for specific patterns
        Dallas is roughly at 32.7-33.0°N, 96.7-97.0°W
        """
        logger.info("Searching for Dallas-area NAIP quads...")
        
        # Get all available quad prefixes
        all_prefixes = self.explore_naip_structure(year)
        
        # Filter for likely Dallas-area prefixes
        # USGS quads for Dallas typically fall in certain ranges
        dallas_quads = []
        
        for prefix in all_prefixes:
            # Try to parse the prefix as a number
            try:
                quad_num = int(prefix)
                # Dallas-area quads are typically in these ranges
                # Based on USGS quadrangle numbering for Texas
                if (32095 <= quad_num <= 33098):
                    dallas_quads.append(prefix)
            except:
                continue
                
        logger.info(f"Found {len(dallas_quads)} potential Dallas-area quads")
        return dallas_quads
    
    def get_quad_details(self, quad_prefix, year=2022):
        """
        Get details about files in a specific quad
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket='naip-analytic',
                Prefix=f'tx/{year}/60cm/rgbir_cog/{quad_prefix}/',
                MaxKeys=20,
                RequestPayer='requester'
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    filename = obj['Key'].split('/')[-1]
                    if filename.endswith('.tif'):
                        files.append(filename)
                        
            return files
            
        except Exception as e:
            logger.debug(f"Error getting details for {quad_prefix}: {e}")
            return []
    
    def find_quads_by_name_pattern(self, year=2022):
        """
        Search for quads that match Dallas coordinates
        NAIP files follow pattern: m_[QQQQQ][QQ]_[quadrant]_[zone]_[resolution]_[date].tif
        where QQQQQ is the USGS quadrangle number
        """
        logger.info("Searching for Dallas quads by examining actual files...")
        
        dallas_quads = self.find_dallas_area_quads(year)
        
        quad_details = {}
        for quad in dallas_quads:
            files = self.get_quad_details(quad, year)
            if files:
                quad_details[quad] = {
                    'file_count': len(files),
                    'sample_files': files[:4],
                    'quad_id': quad
                }
                
                # Parse the quad ID from filename
                if files:
                    # Example: m_3209661_ne_14_060_20220815.tif
                    # The quad ID is 3209661
                    sample = files[0]
                    parts = sample.split('_')
                    if len(parts) >= 3:
                        full_quad_id = parts[1]
                        quad_details[quad]['full_quad_id'] = full_quad_id
        
        return quad_details
    
    def find_specific_area(self, lat, lon, year=2022):
        """
        Find quads near specific coordinates
        This is approximate - actual coverage requires checking the files
        """
        logger.info(f"\nSearching for NAIP coverage near {lat:.4f}°N, {lon:.4f}°W")
        
        # Get all Dallas-area quads
        quad_details = self.find_quads_by_name_pattern(year)
        
        # For Dallas zip codes, we expect quads in certain ranges
        # Let's identify the most likely candidates
        likely_quads = []
        
        for quad_prefix, details in quad_details.items():
            # Check if this could be near our coordinates
            # This is approximate - you'd need the actual quad boundaries for precision
            try:
                quad_num = int(quad_prefix)
                
                # Rough filtering based on Dallas area
                # Northern Dallas (like 75287) would be in the northern quad range
                if lat > 32.9:  # Northern Dallas
                    if 32096 <= quad_num <= 32097 or 33096 <= quad_num <= 33097:
                        likely_quads.append((quad_prefix, details))
                else:  # Southern/Central Dallas
                    if 32095 <= quad_num <= 32096 or 33095 <= quad_num <= 33096:
                        likely_quads.append((quad_prefix, details))
                        
            except:
                continue
        
        return likely_quads
    
    def search_all_years(self, lat, lon):
        """Search across all available years"""
        years = [2020, 2022, 2024]
        all_results = {}
        
        for year in years:
            logger.info(f"\n{'='*50}")
            logger.info(f"Searching year {year}")
            logger.info(f"{'='*50}")
            
            quads = self.find_specific_area(lat, lon, year)
            if quads:
                all_results[year] = quads
                
        return all_results


def main():
    if len(sys.argv) != 2:
        print("Usage: python find_naip_quads.py <zip_code>")
        sys.exit(1)
    
    zip_code = sys.argv[1]
    
    # Dallas area zip codes with known coordinates
    zip_coords = {
        '75287': (33.0005, -96.8314),  # North Dallas
        '75201': (32.7831, -96.8067),  # Downtown
        '75202': (32.7806, -96.7970),  # Downtown
        '75203': (32.7487, -96.7919),  # South Dallas
        '75204': (32.8029, -96.7828),  # Uptown
    }
    
    if zip_code not in zip_coords:
        print(f"Zip code {zip_code} not in database. Add coordinates to continue.")
        sys.exit(1)
    
    finder = NAIPQuadFinder()
    coords = zip_coords[zip_code]
    
    # Search all years
    results = finder.search_all_years(coords[0], coords[1])
    
    # Extract unique quad IDs
    all_quad_ids = set()
    quad_summary = {}
    
    for year, quads in results.items():
        logger.info(f"\nYear {year} summary:")
        for quad_prefix, details in quads:
            logger.info(f"  Quad {quad_prefix}: {details['file_count']} files")
            if 'full_quad_id' in details:
                all_quad_ids.add(details['full_quad_id'])
                quad_summary[details['full_quad_id']] = details['sample_files']
            
            # Show sample files
            for f in details['sample_files'][:2]:
                logger.info(f"    - {f}")
    
    # Save results
    output = {
        'zip_code': zip_code,
        'coordinates': {'lat': coords[0], 'lon': coords[1]},
        'quad_ids': sorted(list(all_quad_ids)),
        'quad_details': quad_summary,
        'search_results': {str(year): [(q[0], q[1]) for q in quads] 
                          for year, quads in results.items()}
    }
    
    output_file = f'naip_quads_{zip_code}.json'
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)
    
    print(f"\n{'='*50}")
    print(f"Results saved to {output_file}")
    print(f"Found {len(all_quad_ids)} unique quad IDs for zip code {zip_code}")
    
    if all_quad_ids:
        print(f"\nExtracted quad IDs:")
        for qid in sorted(all_quad_ids)[:10]:  # Show first 10
            print(f"  - {qid}")
            
        print(f"\nUpdate your dallas_zip_quad_mapping.json with:")
        print(f'"quad_ids": {json.dumps(sorted(list(all_quad_ids))[:4], indent=2)}')
        print("\nNote: Verify coverage using USGS EarthExplorer before production use")


if __name__ == "__main__":
    main()
