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
    --spatial-index    Create spatial indexes for the output GeoPackage
    --keep-separate    Keep input layers separate instead of merging them

Requirements:
    - GDAL/OGR tools must be installed and accessible in the system PATH
    - Python packages: click

Example usage:
    python gpkg_consolidator.py /input/gpkg /path/to/output.gpkg
    python gpkg_consolidator.py /input/gpkg /path/to/output.gpkg --overwrite --spatial-index
    python gpkg_consolidator.py /path/to/input/gpkg /path/to/output.gpkg --keep-separate

Author: Ian Grant
AI Assistance: Claude 3.5 Sonnet (Anthropic)
Date: 2024-09-04
Version: 0.1.0
"""

import subprocess
from pathlib import Path
from typing import List, Set

import click


# Utility functions
def validate_geopackage(gpkg: Path) -> bool:
    """
    Check if a file is a valid GeoPackage.

    Args:
    gpkg (Path): Path to the GeoPackage file.

    Returns:
    bool: True if valid, False otherwise.
    """
    try:
        subprocess.run(
            ["ogrinfo", "-so", "-q", str(gpkg)],
            check=True,
            capture_output=True,
        )
        return True
    except subprocess.CalledProcessError:
        return False


def create_spatial_indexes(gpkg: Path) -> None:
    """
    Create spatial indexes for all layers in a GeoPackage.

    Args:
    gpkg (Path): Path to the GeoPackage file.
    """
    click.echo(f"Creating spatial indexes for {gpkg}")
    subprocess.run(
        [
            "ogrinfo",
            str(gpkg),
            "-sql",
            "SELECT CreateSpatialIndex(TABLE_NAME, GEOMETRY_COLUMN_NAME) "
            "FROM gpkg_geometry_columns",
        ],
        check=True,
    )


def get_layers(gpkg: Path) -> List[str]:
    """
    Get a list of layer names from a GeoPackage.

    Args:
    gpkg (Path): Path to the GeoPackage file.

    Returns:
    List[str]: List of layer names.
    """
    result = subprocess.run(
        ["ogrinfo", "-so", str(gpkg)], capture_output=True, text=True
    )
    layers = [
        line.split(":")[1].strip()
        for line in result.stdout.split("\n")
        if line.startswith("1: ")
    ]
    return layers


@click.command()
@click.argument(
    "input_directory",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
)
@click.argument("output_geopackage", type=click.Path())
@click.option(
    "--overwrite", is_flag=True, help="Overwrite the output file if it exists"
)
@click.option(
    "--spatial-index",
    is_flag=True,
    help="Create spatial indexes for the output GeoPackage",
)
@click.option(
    "--keep-separate",
    is_flag=True,
    help="Keep layers separate instead of merging them (default is to merge)",
)
def consolidate_gpkg(
    input_directory: str,
    output_geopackage: str,
    overwrite: bool,
    spatial_index: bool,
    keep_separate: bool,
) -> None:
    """
    Consolidate multiple GeoPackage files from INPUT_DIRECTORY into a single
    OUTPUT_GEOPACKAGE file.
    """
    # Initialize paths and find GeoPackage files
    input_dir = Path(input_directory)
    output_gpkg = Path(output_geopackage)
    gpkg_files: List[Path] = list(input_dir.glob("*.gpkg"))
    gpkg_count: int = len(gpkg_files)

    # Check if any GeoPackage files were found
    if gpkg_count == 0:
        click.echo(
            "Error: No GeoPackages found in the input directory.", err=True
        )
        return

    click.echo(f"Found {gpkg_count} GeoPackage(s) to process.")

    # Define the output layer name (used in merge mode)
    output_layer: str = output_gpkg.stem

    # Handle existing output file scenarios
    if output_gpkg.exists():
        if overwrite:
            if click.confirm(f"Output file {output_gpkg} exists. Overwrite?"):
                output_gpkg.unlink()
            else:
                click.echo("Operation cancelled.")
                return
        elif not keep_separate:
            # Check if the merged layer already exists in the output file
            existing_layers: List[str] = get_layers(output_gpkg)
            if output_layer in existing_layers:
                if not click.confirm(
                    f"Layer '{output_layer}' already exists in "
                    f"{output_gpkg}. Append to it?"
                ):
                    click.echo("Operation cancelled.")
                    return
            else:
                if not click.confirm(
                    f"Output file {output_gpkg} exists. "
                    f"Create new layer '{output_layer}'?"
                ):
                    click.echo("Operation cancelled.")
                    return

    processed: int = 0
    if not keep_separate:
        # Merge all layers into a single layer
        click.echo(f"Merging all layers into a single layer: {output_layer}")
        for i, gpkg in enumerate(gpkg_files):
            if validate_geopackage(gpkg):
                click.echo(f"Processing ({i + 1}/{gpkg_count}): {gpkg.name}")
                try:
                    if i == 0 and not output_gpkg.exists():
                        # For the first file, create the output GeoPackage
                        subprocess.run(
                            [
                                "ogr2ogr",
                                "-f",
                                "GPKG",
                                str(output_gpkg),
                                str(gpkg),
                                "-nln",
                                output_layer,
                            ],
                            check=True,
                        )
                    else:
                        # For subsequent files, append to the existing layer
                        subprocess.run(
                            [
                                "ogr2ogr",
                                "-update",
                                "-append",
                                str(output_gpkg),
                                str(gpkg),
                                "-nln",
                                output_layer,
                            ],
                            check=True,
                        )
                    processed += 1
                    click.echo(f"Successfully processed: {gpkg.name}")
                except subprocess.CalledProcessError:
                    click.echo(f"Error processing: {gpkg.name}", err=True)
            else:
                click.echo(
                    f"Skipping invalid GeoPackage: {gpkg.name}", err=True
                )
    else:
        # Keep layers separate
        if not output_gpkg.exists():
            click.echo(f"Creating output GeoPackage: {output_gpkg}")
            try:
                # Create an empty GeoPackage if it doesn't exist
                subprocess.run(
                    [
                        "ogr2ogr",
                        "-f",
                        "GPKG",
                        str(output_gpkg),
                        "/vsimem/empty.gpkg",
                    ],
                    check=True,
                )
            except subprocess.CalledProcessError:
                click.echo(
                    "Error: Failed to create output GeoPackage.", err=True
                )
                return

        # Get existing layers in the output GeoPackage
        existing_layers: Set[str] = set(get_layers(output_gpkg))

        # Process each input GeoPackage
        for i, gpkg in enumerate(gpkg_files):
            if validate_geopackage(gpkg):
                click.echo(f"Processing ({i + 1}/{gpkg_count}): {gpkg.name}")
                input_layers: List[str] = get_layers(gpkg)
                for layer in input_layers:
                    new_layer_name: str = layer
                    suffix: int = 2
                    # Handle duplicate layer names
                    while new_layer_name in existing_layers:
                        new_layer_name = f"{layer}_{suffix}"
                        suffix += 1
                    try:
                        # Add the layer to the output GeoPackage
                        subprocess.run(
                            [
                                "ogr2ogr",
                                "-update",
                                "-append",
                                str(output_gpkg),
                                str(gpkg),
                                layer,
                                "-nln",
                                new_layer_name,
                            ],
                            check=True,
                        )
                        existing_layers.add(new_layer_name)
                        if new_layer_name != layer:
                            click.echo(f"Added layer as: {new_layer_name}")
                        else:
                            click.echo(f"Added layer: {layer}")
                    except subprocess.CalledProcessError:
                        click.echo(
                            f"Error processing layer: {layer}", err=True
                        )
                processed += 1
                click.echo(f"Successfully processed: {gpkg.name}")
            else:
                click.echo(
                    f"Skipping invalid GeoPackage: {gpkg.name}", err=True
                )

    # Print summary of processing
    if processed == gpkg_count:
        click.echo(
            f"Consolidation complete. All {processed} GeoPackage(s) processed."
        )
    else:
        click.echo(
            f"Consolidation finished with warnings. "
            f"Processed {processed} out of {gpkg_count} GeoPackage(s)."
        )

    # Create spatial indexes if requested
    if spatial_index:
        create_spatial_indexes(output_gpkg)

    click.echo(f"Output: {output_gpkg}")


if __name__ == "__main__":
    consolidate_gpkg()
