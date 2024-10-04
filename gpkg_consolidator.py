"""
GeoPackage Consolidator

This script consolidates multiple GeoPackage files from an input directory
into a single output GeoPackage file using GDAL/OGR tools via Python Click.

Features:
- Merge multiple GeoPackages into one
- Option to keep layers separate or merge them into a single layer
- Create spatial indexes in the output GeoPackage
- Handle duplicate layer names when keeping layers separate
- Validate input GeoPackages before processing

Usage:
    python script_name.py INPUT_DIRECTORY OUTPUT_GEOPACKAGE [OPTIONS]

Arguments:
    INPUT_DIRECTORY    Path to the directory containing input GeoPackages
    OUTPUT_GEOPACKAGE  Path to the output consolidated GeoPackage

Options:
    --overwrite        Overwrite the output file if it exists
    --update           Update existing GeoPackage (creates new layers if not used with --append)
    --append           Append to existing layers with matching names
    --spatial-index    Create spatial indexes for the output GeoPackage
    --keep-separate    Keep input layers separate instead of merging them

Requirements:
    - GDAL/OGR tools must be installed and accessible in the system PATH
    - Python packages: click, tqdm

Example usage:
    python gpkg_consolidator.py /input/gpkg /path/to/output.gpkg
    python gpkg_consolidator.py /input/gpkg /path/to/output.gpkg --append --spatial-index
    python gpkg_consolidator.py /path/to/input/gpkg /path/to/output.gpkg --keep-separate
    python gpkg_consolidator.py /path/to/input/gpkg /path/to/output.gpkg --update --append

Author: Ian Grant
AI Assistance: Claude 3.5 Sonnet (Anthropic)
Date: 2024-09-04
Version: 0.1.0
"""

import subprocess
from pathlib import Path
from typing import List, Callable
import click
from tqdm import tqdm

def validate_geopackage(gpkg: Path) -> bool:
    """Check if a file is a valid GeoPackage."""
    if not gpkg.exists():
        return False
    try:
        subprocess.run(
            ["ogrinfo", "-so", "-q", str(gpkg)],
            check=True, capture_output=True
        )
        return True
    except subprocess.CalledProcessError:
        return False

def get_layers(gpkg: Path) -> List[str]:
    """Get a list of layer names from a GeoPackage."""
    if not gpkg.exists():
        return []
    try:
        result = subprocess.run(
            ["ogrinfo", "-so", str(gpkg)],
            capture_output=True, text=True, check=True
        )
        return [
            # TODO: make more robust by using ogr
            line.split(":")[1].strip().split(" ")[0]
            for line in result.stdout.splitlines()
            if line.startswith("Layer name:")
        ]
    except subprocess.CalledProcessError as e:
        tqdm.write(f"Error getting layers from {gpkg}: {e.stderr}")
        return []

def get_unique_layer_name(base_name: str, output_gpkg: Path) -> str:
    """Generate a unique layer name to avoid conflicts in the output GeoPackage."""
    existing_layers = set(get_layers(output_gpkg))
    if base_name not in existing_layers:
        return base_name
    suffix = 2
    while f"{base_name}_{suffix}" in existing_layers:
        suffix += 1
    return f"{base_name}_{suffix}"

def generate_ogr_constructor(keep_separate: bool,
                             output_gpkg: Path,
                             append: bool,
                             update: bool) -> Callable:
    """Generate an OGR command constructor function based on CLI options."""
    def construct_ogr_command(input_gpkg: Path,
                              input_layer: str) -> List[str]:
        if keep_separate:
            if append:
                output_layer_name = input_layer
            else:
                output_layer_name = get_unique_layer_name(input_layer,
                                                          output_gpkg)
        else:
            if append:
                output_layer_name = output_gpkg.stem
            else:
                output_layer_name = get_unique_layer_name(output_gpkg.stem,
                                                          output_gpkg)
        cmd = [
            "ogr2ogr",
            "-f", "GPKG",
        ]
        if update:
            cmd.append("-update")
        cmd.extend([
            str(output_gpkg),
            str(input_gpkg),
            '-sql', f'SELECT * FROM "{input_layer}"',
            "-nln", output_layer_name
        ])
        if append:
            cmd.append("-append")
        return cmd
    return construct_ogr_command

def process_geopackage(input_gpkg: Path,
                       construct_ogr_command: Callable) -> None:
    """Process a single input GeoPackage, writing its layers to the output."""
    input_layers = get_layers(input_gpkg)
    
    for layer in input_layers:
        cmd = construct_ogr_command(input_gpkg, layer)
        try:
            subprocess.run(cmd, check=True, capture_output=True)
            tqdm.write(f"Processed layer: {layer} from {input_gpkg.name}")
        except subprocess.CalledProcessError as e:
            tqdm.write(f"Error processing layer: {layer} from {input_gpkg.name}", err=True)
            tqdm.write(f"Error message: {e.stderr.decode()}", err=True)

def create_spatial_indexes(gpkg: Path) -> None:
    """Create spatial indexes for all layers in a GeoPackage."""
    tqdm.write(f"Creating spatial indexes for {gpkg}")
    try:
        subprocess.run(
            [
                "ogrinfo",
                str(gpkg),
                "-sql",
                "SELECT CreateSpatialIndex(TABLE_NAME, GEOMETRY_COLUMN_NAME) "
                "FROM gpkg_geometry_columns",
            ],
            check=True,
            capture_output=True
        )
    except subprocess.CalledProcessError as e:
        tqdm.write(f"Error creating spatial indexes: {e.stderr.decode()}", err=True)

@click.command()
@click.argument(
    "input_directory",
    type=click.Path(exists=True, file_okay=False, dir_okay=True)
)
@click.argument("output_geopackage", type=click.Path())
@click.option(
    "--overwrite",
    is_flag=True,
    help="Overwrite the output file if it exists"
)
@click.option(
    "--update",
    is_flag=True,
    help="Update existing GeoPackage (creates new layers if not used with --append)"
)
@click.option(
    "--append",
    is_flag=True,
    help="Append to existing layers in the output GeoPackage"
)
@click.option(
    "--spatial-index",
    is_flag=True,
    help="Create spatial indexes for the output GeoPackage"
)
@click.option(
    "--keep-separate",
    is_flag=True,
    help="Keep layers separate instead of merging them"
)
def consolidate_gpkg(
    input_directory: str,
    output_geopackage: str,
    overwrite: bool,
    update: bool,
    append: bool,
    spatial_index: bool,
    keep_separate: bool
) -> None:
    """Consolidate multiple GeoPackages into a single output GeoPackage."""
    input_dir = Path(input_directory)
    output_gpkg = Path(output_geopackage)
    gpkg_files: List[Path] = list(input_dir.glob("*.gpkg"))
    gpkg_count: int = len(gpkg_files)

    if gpkg_count == 0:
        tqdm.write("Error: No GeoPackages found in the input directory.", err=True)
        return

    if output_gpkg.exists():
        if overwrite:
            output_gpkg.unlink()
        elif not update:
            tqdm.write(f"Error: Output file {output_gpkg} already exists. Use --overwrite to replace or --update to add to it.", err=True)
            return

    construct_ogr_command = generate_ogr_constructor(keep_separate, output_gpkg, append, update)

    with tqdm(total=gpkg_count, desc="Processing GeoPackages") as pbar:
        for gpkg in gpkg_files:
            if validate_geopackage(gpkg):
                process_geopackage(gpkg, construct_ogr_command)
            else:
                tqdm.write(f"Skipping invalid GeoPackage: {gpkg.name}", err=True)
            pbar.update(1)

    if spatial_index:
        create_spatial_indexes(output_gpkg)

    tqdm.write(f"Consolidation complete. Output: {output_gpkg}")

if __name__ == "__main__":
    consolidate_gpkg()
