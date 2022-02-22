import os
from datetime import timedelta

from hypothesis import settings

on_azure_pipelines = bool(os.getenv('TF_BUILD', False))
max_examples = settings.default.max_examples
settings.register_profile('default',
                          deadline=(timedelta(hours=1) / max_examples
                                    if on_azure_pipelines
                                    else None),
                          max_examples=max_examples)
