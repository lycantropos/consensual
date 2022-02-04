import os

from hypothesis import settings

on_azure_pipelines = bool(os.getenv('TF_BUILD', False))
settings.register_profile('default',
                          max_examples=(settings.default.max_examples // 5
                                        if on_azure_pipelines
                                        else settings.default.max_examples),
                          deadline=None)
