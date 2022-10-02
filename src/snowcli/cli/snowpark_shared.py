import click
import os
from pathlib import Path
import tempfile
from rich import print
import typer

from snowcli import utils, config
from snowcli.config import AppConfig
from snowcli.utils import print_db_cursor, generate_deploy_stage_name, print_list_tuples

def snowpark_create(type: str, environment: str, name: str, file: Path, handler: str, input_parameters: str, return_type: str, overwrite: bool, execute_as_caller: bool = False):
    env_conf = AppConfig().config.get(environment)
    if env_conf is None:
        print("The {environment} environment is not configured in app.toml yet, please run `snow configure dev` first before continuing.")
        raise typer.Abort()

    if config.isAuth():
        config.connectToSnowflake()
        deploy_dict = utils.getDeployNames(env_conf['database'], env_conf['schema'], generate_deploy_stage_name(name, input_parameters))
        print('Uploading deployment file to stage...')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_app_zip_path = utils.prepareAppZip(file, temp_dir)
            config.snowflake_connection.uploadFileToStage(
                file_path=temp_app_zip_path, destination_stage=deploy_dict['stage'], path=deploy_dict['directory'], overwrite=overwrite, role=env_conf['role'])
        packages = utils.getSnowflakePackages()
        print(f'Creating {type}...')
        match type:
            case 'function':
                results =  config.snowflake_connection.createFunction(name=name, inputParameters=input_parameters,
                                                            returnType=return_type,
                                                            handler=handler,
                                                            imports=deploy_dict['full_path'],
                                                            database=env_conf['database'],
                                                            schema=env_conf['schema'],
                                                            role=env_conf['role'],
                                                            warehouse=env_conf['warehouse'],
                                                            overwrite=overwrite,
                                                            packages=packages,
                                                            )
            case 'procedure':
                results =  config.snowflake_connection.createProcedure(name=name, inputParameters=input_parameters,
                                                            returnType=return_type,
                                                            handler=handler,
                                                            imports=deploy_dict['full_path'],
                                                            database=env_conf['database'],
                                                            schema=env_conf['schema'],
                                                            role=env_conf['role'],
                                                            warehouse=env_conf['warehouse'],
                                                            overwrite=overwrite,
                                                            packages=packages,
                                                            execute_as_caller=execute_as_caller
                                                            )
            case _:
                raise typer.Abort()
        print_list_tuples(results)

def snowpark_update(type: str, environment: str, name: str, file: Path, handler: str, input_parameters: str, return_type: str, replace: bool, execute_as_caller: bool = False):
    env_conf: dict = AppConfig().config.get(environment)  # type: ignore
    if env_conf is None:
        print("The {environment} environment is not configured in app.toml yet, please run `snow configure dev` first before continuing.")
        raise typer.Abort()
    if config.isAuth():
        config.connectToSnowflake()
        
        try:
            print(f'Updating {type} {name}...')
            match type:
                case 'function':
                    resource_details = config.snowflake_connection.describeFunction(
                        name=name, inputParameters=input_parameters, database=env_conf['database'], schema=env_conf['schema'], role=env_conf['role'], warehouse=env_conf['warehouse'])
                case 'procedure':
                    resource_details = config.snowflake_connection.describeProcedure(
                        name=name, inputParameters=input_parameters, database=env_conf['database'], schema=env_conf['schema'], role=env_conf['role'], warehouse=env_conf['warehouse'])
            print(f'Checking if any new packages to update...')
            resource_json = utils.convertResourceDetailsToDict(resource_details)  # type: ignore
            anaconda_packages = resource_json['packages']
            print(
                f'Found {len(anaconda_packages)} defined Anaconda packages in deployed {type}...')
            print(
                f'Checking if any packages defined or missing from requirements.snowflake.txt...')
            updatedPackageList = utils.getSnowflakePackagesDelta(anaconda_packages)
        except:
            typer.echo(f'Existing {type} not found, creating new {type}...')
            updatedPackageList = utils.getSnowflakePackagesDelta([])
            replace = True
        
        finally:
            deploy_dict = utils.getDeployNames(env_conf['database'], env_conf['schema'], generate_deploy_stage_name(name, input_parameters))
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_app_zip_path = utils.prepareAppZip(file, temp_dir)
                deploy_response = config.snowflake_connection.uploadFileToStage(
                    file_path=temp_app_zip_path, destination_stage=deploy_dict['stage'], path=deploy_dict['directory'], overwrite=True, role=env_conf['role'])
            print(
                f'{deploy_response} uploaded to stage {deploy_dict["full_path"]}')
            
            if updatedPackageList or replace:
                print(f'Replacing {type} with updated values...')
                match type:
                    case 'function':
                        config.snowflake_connection.createFunction(
                            name=name,
                            inputParameters=input_parameters,
                            returnType=return_type,
                            handler=handler,
                            imports=deploy_dict['full_path'],
                            database=env_conf['database'],
                            schema=env_conf['schema'],
                            role=env_conf['role'],
                            warehouse=env_conf['warehouse'],
                            overwrite=True,
                            packages=updatedPackageList)
                    case 'procedure':
                        config.snowflake_connection.createProcedure(
                            name=name,
                            inputParameters=input_parameters,
                            returnType=return_type,
                            handler=handler,
                            imports=deploy_dict['full_path'],
                            database=env_conf['database'],
                            schema=env_conf['schema'],
                            role=env_conf['role'],
                            warehouse=env_conf['warehouse'],
                            overwrite=True,
                            packages=updatedPackageList,
                            execute_as_caller=execute_as_caller)
                print(
                    f'{type.capitalize()} {name} updated with new packages. Deployment complete!')
            else:
                print(f'No packages to update. Deployment complete!')

def snowpark_package():
    print('Resolving any requirements from requirements.txt...')
    requirements = utils.parseRequirements()
    pack_dir: str = None  # type: ignore
    if requirements:
        print('Comparing provided packages from Snowflake Anaconda...')
        parsedRequirements = utils.parseAnacondaPackages(requirements)
        if not parsedRequirements['other']:
            print('No packages to manually resolve')
        if parsedRequirements['other']:
            print('Writing requirements.other.txt...')
            with open('requirements.other.txt', 'w') as f:
                for package in parsedRequirements['other']:
                    f.write(package + '\n')
        # if requirements.other.txt exists
        if os.path.isfile('requirements.other.txt'):
            if click.confirm('Do you want to try to download non-Anaconda packages?', default=True):
                print('Installing non-Anaconda packages...')
                if utils.installPackages('requirements.other.txt'):
                    pack_dir = '.packages'
        # write requirements.snowflake.txt file
        if parsedRequirements['snowflake']:
            print('Writing requirements.snowflake.txt file...')
            with open('requirements.snowflake.txt', 'w') as f:
                for package in parsedRequirements['snowflake']:
                    f.write(package + '\n')
        if pack_dir:
            utils.recursiveZipPackagesDir(pack_dir, 'app.zip')
        else:
            utils.standardZipDir('app.zip')
    else:
        utils.standardZipDir('app.zip')
    print('\n\nDeployment package now ready: app.zip')

def snowpark_execute(type: str, environment: str, select: str):
    env_conf = AppConfig().config.get(environment)
    if env_conf is None:
        print("The {environment} environment is not configured in app.toml yet, please run `snow configure dev` first before continuing.")
        raise typer.Abort()
    if config.isAuth():
        config.connectToSnowflake()
        match type:
            case 'function':
                results = config.snowflake_connection.executeFunction(
                    function=select, database=env_conf['database'], schema=env_conf['schema'], role=env_conf['role'], warehouse=env_conf['warehouse'])
            case 'procedure':
                results = config.snowflake_connection.executeProcedure(
                    procedure=select, database=env_conf['database'], schema=env_conf['schema'], role=env_conf['role'], warehouse=env_conf['warehouse'])
            case _:
                raise typer.Abort()
        print_db_cursor(results)

def snowpark_describe(type: str, environment: str, name: str, input_parameters: str, signature: str):
    env_conf = AppConfig().config.get(environment)
    if env_conf is None:
        print("The {environment} environment is not configured in app.toml yet, please run `snow configure dev` first before continuing.")
        raise typer.Abort()
    
    if config.isAuth():
        config.connectToSnowflake()
        if signature == '':
            if name == '' and input_parameters == '':
                typer.BadParameter('Please provide either a function name and input parameters or a function signture')
            signature = name + config.snowflake_connection.generate_signature_from_params(input_parameters)
        match type:
            case 'function':
                results = config.snowflake_connection.describeFunction(
                    signature=signature, database=env_conf['database'], schema=env_conf['schema'], role=env_conf['role'], warehouse=env_conf['warehouse'])
            case 'procedure':
                results = config.snowflake_connection.describeProcedure(
                    signature=signature, database=env_conf['database'], schema=env_conf['schema'], role=env_conf['role'], warehouse=env_conf['warehouse'])
            case _:
                raise typer.Abort()
        print_list_tuples(results)