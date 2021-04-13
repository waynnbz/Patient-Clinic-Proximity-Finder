import pandas as pd
import numpy as np
import ast
import sys

from match import map_files
from geocode import geocode_files


def load_files(P_FILE, C_FILE):
    try:
        p_df = pd.read_csv(P_FILE)
        c_df = pd.read_csv(C_FILE)
    except:
        print("Fail to load data, please check input data path is correct")
        sys.exit(1)

    return p_df, c_df

import click

@click.command()
@click.option('patients_file', '--patients', default='./data/patients.csv', type=click.Path(exists=True),
             help='Patients.csv file path')
@click.option('clinics_file', '--clinics', default='./data/clinics.csv', type=click.Path(exists=True),
             help='Clinics.csv file path')
@click.option('output_path', '--output', default='./output', type=click.Path(),
             help='Output directory')
def main(patients_file, clinics_file, output_path):
    
    click.echo('Loading data files...')
    P_df, C_df = load_files(patients_file, clinics_file)
    
    click.echo('Geocoding...')
    P_df, C_df = geocode_files(P_df, C_df, output_path)
    
    click.echo('Calculating travel distance and matching...')
    PC_mapped = map_files(P_df, C_df, output_path)

    
if __name__ == '__main__':
    main()