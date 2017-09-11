'''
Reformatting bcsd raw data to fill holes.

version 1.0 - initial release

'''

import os
import logging

from jrnr.jrnr import slurm_runner

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)

logger = logging.getLogger('uploader')
logger.setLevel('DEBUG')

__author__ = 'Michael Delgado'
__contact__ = 'mdelgado@rhg.com'
__version__ = '1.0'


BCSD_orig_files = (
    '/global/scratch/jiacany/nasa_bcsd/raw_data/{scenario}/{model}/' +
    '{variable}/' +
    '{variable}_day_BCSD_{scenario}_r1i1p1_{model}_{year}.nc')

WRITE_PATH = (
    '/global/scratch/jsimcock//gcp/climate/' +
    'nasa_bcsd/reformatted/{variable}/{scenario}/{model}/{year}/' +
    '{version}.nc4')

description = '\n\n'.join(
        map(lambda s: ' '.join(s.split('\n')),
            __doc__.strip().split('\n\n')))

oneline = description.split('\n')[0]

ADDITIONAL_METADATA = dict(
    oneline=oneline,
    description=description,
    author=__author__,
    contact=__contact__,
    version=__version__,
    repo='https://gitlab.com/Climate/climate-transforms/',
    file='/fill_holes_bcsd.py',
    execute='python fill_holes_bcsd.py run',
    project='gcp',
    team='climate',
    frequency='daily',
    dependencies='')



def format_docstr(docstr):
    pars = docstr.split('\n\n')
    pars = [
        ' '.join(map(lambda s: s.strip(), par.split('\n'))) for par in pars]

    return '\n\n'.join(pars)

VARS = ([dict(variable='tasmax') +
         dict(variable='tasmin') +
         dict(variable='tas')
         ])

PERIODS = (
    [dict(scenario='historical', year=y) for y in range(1981, 2006)] +
    [dict(scenario='rcp45',  year=y) for y in range(2006, 2100)] +
    [dict(scenario='rcp85', year=y) for y in range(2006, 2100)])

MODELS = list(map(lambda x: dict(model=x), [
    'ACCESS1-0',
    'bcc-csm1-1',
    'BNU-ESM',
    'CanESM2',
    'CCSM4',
    'CESM1-BGC',
    'CNRM-CM5',
    'CSIRO-Mk3-6-0',
    'GFDL-CM3',
    'GFDL-ESM2G',
    'GFDL-ESM2M',
    'IPSL-CM5A-LR',
    'IPSL-CM5A-MR',
    'MIROC-ESM-CHEM',
    'MIROC-ESM',
    'MIROC5',
    'MPI-ESM-LR',
    'MPI-ESM-MR',
    'MRI-CGCM3',
    'inmcm4',
    'NorESM1-M'
    ]))


JOBS = [MODELS, PERIODS, VARS]


@slurm_runner(filepath=__file__, job_spec=JOB_SPEC)
def fill_holes_bcsd(
        metadata,
        variable,
        variable,
        scenario,
        year,
        model,
        interactive=False):

    import xarray as xr
    import metacsv

    from climate_toolbox.climate_toolbox import (
        load_bcsd)

    # Add to job metadata
    metadata.update(ADDITIONAL_METADATA)


    read_file = BCSD_orig_files.format(**metadata)
    write_file = WRITE_PATH.format(**metadata)

    # do not duplicate
    if os.path.isfile(write_file) and not interactive:
        return

    # Get transformed data

    with xr.open_dataset(fp) as ds:
        ds.load()

    logger.debug('year {} - attempting to read file "{}"'.format(year, fp))

    ds = load_bcsd(ds, variable, broadcast_dims=('time',))

    varattrs = {var: dict(ds[var].attrs) for var in ds.data_vars.keys()}

    # Update netCDF metadata
    ds.attrs.update(**{
        k: str(v) for k, v in metadata.items() if k in INCLUDED_METADATA})
    ds.attrs.update(ADDITIONAL_METADATA)

    attrs = dict(ds.attrs)
    attrs['file_dependencies'] = file_dependencies

    for var, vattrs in varattrs.items():
        ds[var].attrs.update(vattrs)

    if interactive:
        return ds

    # Write output
    if not os.path.isdir(os.path.dirname(write_file)):
        logger.debug(
            'attempting to create_directory "{}"'
            .format(os.path.dirname(write_file)))

        os.makedirs(os.path.dirname(write_file))

    logger.debug('attempting to write to file "{}"'.format(write_file))

    ds.to_netcdf(write_file)

    metacsv.to_header(
        write_file.replace('.nc4', '.fgh'),
        attrs=dict(attrs),
        variables=varattrs)

    logger.debug('job done')


if __name__ == '__main__':
    fill_holes_bcsd()
