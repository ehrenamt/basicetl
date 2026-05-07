"""Implementation for the SubserviceController class."""

# third-party imports
import yaml

# project imports (submodules and options classes)
from basicetl.controllers.submodules.extract import extract_based_on_source
from basicetl.controllers.extract_options import ExtractOptions
from basicetl.controllers.transform_options import TransformOptions

from basicetl.utils.custom_exceptions import ConfigNotFoundException


class SubmoduleController:
    """
    The SubserviceController class handles errors and enforces configurations.
    """


    def __init__(self):
        self.jointype = True # TODO placeholder, will replace


    def run_extract(self, config_path):
        """
        Resolves extraction config settings and then passes them to the extraction subservice.
        """

        options = self._resolve_new_extract_config(config_path)

        extract_based_on_source(source="fakesource", options=options)

        return True


    def run_transform(self, config_path: str):
        """
        Resolves extraction config settings and then passes them to the extraction subservice.
        """
        options = self._resolve_new_transform_config(config_path)


    # TODO manage SQL connections for efficiency.
    # def run_load(self):
    #     self._resolve_config()


    def _resolve_new_extract_config(self, config_path: str):
        try:

            with open(config_path, 'r', encoding='utf-8') as config_file:
                config_dict = yaml.safe_load(config_file)

                # Debug
                print(config_dict)

                extract_config = config_dict.get('extract')


                if extract_config is None:
                    raise ConfigNotFoundException("'Extract' option in config not found.")

                output_type = extract_config.get('output_type')
                save_to_disk = extract_config.get('save_to_disk')
                chunk_size = extract_config.get('chunk_size')
                sources = extract_config.get('sources')

                # Debug
                print("Extract Configuration:")
                print(f"Output Type: {output_type}")
                print(f"Save to Disk: {save_to_disk}")
                print(f"Chunk Size: {chunk_size}")
                print(f"Sources: {sources}")


        except FileNotFoundError as e:
            print(f"Unable to read from '{config_file}': {e}")

        except ConfigNotFoundException as e:
            print(f"{e}")

        options = ExtractOptions()

        options.save_to_disk = save_to_disk
        options.output_type = output_type
        options.chunk_size = chunk_size
        options.sources = sources

        return options


    def _resolve_new_transform_config(self, config_path: str):
        try:

            with open(config_path, 'r', encoding='utf-8') as config_file:
                config_dict = yaml.safe_load(config_file)

                # Debug
                print(config_dict)


        except FileNotFoundError as e:
            print(f"Unable to read from '{config_file}': {e}")

        except ConfigNotFoundException as e:
            print(f"{e}")

        options = TransformOptions()

        return options


# Our technology forces us to live mythically.
