import re
import docker
import os

VEP_DOCKER_IMAGE = "ensemblorg/ensembl-vep:release_101.0"

CONTAINER_INPUT_FILE = "/opt/vep/src/ensembl-vep/input.vcf"
CONTAINER_OUTPUT_FILE = "/opt/vep/src/ensembl-vep/output.vcf"

CONTAINER_DIR_CACHE = "/opt/vep/src/ensembl-vep/vep_data"


input_file = "/data-cbl/agarcia/cbl04/vcodeprojects/vep_docker_py/vep_docker_py/inputs/SEDv_raw_clean.vcf"
output_file = "/data-cbl/agarcia/cbl04/vcodeprojects/vep_docker_py/vep_docker_py/inputs/SEDv_raw_vep.vcf"
dir_cache = "/data-cbl/agarcia/vep_annotation_37/vep_data"

client = docker.from_env()

def image_pull(docker_image_name):
    try:
        client.images.get(docker_image_name)
        print("Image found!")
    except docker.errors.ImageNotFound:
        print(f"No image found \n Proceeding to download {docker_image_name}")
        
        client.images.pull(docker_image_name.split(":")[0],docker_image_name.split(":")[1])


def run(
    input_file,
    output_file,
    dir_cache,
    species =  "homo_sapiens",
    assembly = "GRCh37",
    format="vcf",
    fork = 50,
    cache = True,
    everything = True,
    offline = True,
    compress_output = "bgzip",
    verbose = True
):
    """Wrapper around Variant Effect Predictor.

    Args:
        input_file (str): Input vcf path
        output_file (str): Output annotated vcf path
        dir_cache (str): Vep cache folder, must contain /homo_sapiens folder
        species (str, optional): [description]. Defaults to "homo_sapiens".
        assembly (str, optional): Assembly used for the annotation. Defaults to "GRCh37".
        format (str, optional): [description]. Defaults to "vcf".
        fork (int, optional): Number of threads used. Defaults to 50.
        cache (bool, optional): [description]. Defaults to True.
        everything (bool, optional): [description]. Defaults to True.
        offline (bool, optional): [description]. Defaults to True.
        compress_output (str, optional): [description]. Defaults to "bgzip".
        verbose (bool, optional): [description]. Defaults to True.
    """

    numeric_params = [
        f"-{key} {value}"
        for key, value in {
            "i": CONTAINER_INPUT_FILE ,
            "o": CONTAINER_OUTPUT_FILE,
            "-species": species,
            "a": assembly,
            "-dir_cache": CONTAINER_DIR_CACHE,
            "-format": format,
            "-fork": fork,
            "-compress_output": compress_output,
            "-cache_version": "101"
        }.items()
        if value
    ]

    bool_params = [
        f"-{key}"
        for key, value in {
            "-offline ": offline,
            "e": everything,
            "-cache" : cache,
            "v": True,
            "-no_stats": True,
            "-force_overwrite": True
        }.items()
        if value
    ]

    cmd_args = " ".join(["./vep" , " ".join((*numeric_params, *bool_params))])
    #cmd_args =f"/bin/bash -c 'touch {CONTAINER_OUTPUT_FILE} ; ls'"

    
    run_docker_container(input_file, output_file, dir_cache, cmd_args, verbose)
    
    print("Done!")


def run_docker_container(input_file, output_file, dir_cache, cmd_args, verbose=True):
    client = docker.from_env()
    #CONTAINER_DIR_CACHE = os.path.join("/opt/vep/.vep/", os.path.split(dir_cache)[1])

    try:
        touch(output_file)

        container = client.containers.run(
            VEP_DOCKER_IMAGE,
            cmd_args,
            volumes={
                input_file: {"bind": CONTAINER_INPUT_FILE, "mode": "ro"},
                output_file: {"bind": CONTAINER_OUTPUT_FILE, "mode": "rw"},
                dir_cache: {"bind" : CONTAINER_DIR_CACHE, "mode" : "ro"},
            },
            detach=True,
            
        )

        if verbose:
            for line in container.logs(stream=True):
                line = line.strip().decode("utf-8")
                if re.fullmatch(r"[a-zA-Z]+\s[a-zA-Z]+: \d\d?.\d\d%", line):
                    print(line, flush=True, end="\r")
                else:
                    print(line, flush=True)
            print()

        container.wait()
        container.remove()

    finally:
        client.close()


def touch(filepath):
    open(filepath, "a").close()



