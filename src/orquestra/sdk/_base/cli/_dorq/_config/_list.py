################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

"""
Code for 'orq login --list'.
"""

from .. import _repos
from .._ui import _presenters, _prompts


class Action:
    """
    Encapsulates app-related logic for handling `orq login --list`.
    """

    def __init__(
        self,
        exception_presenter=_presenters.WrappedCorqOutputPresenter(),
        config_presenter=_presenters.ConfigPresenter(),
        config_repo=_repos.ConfigRepo(),
        prompter=_prompts.Prompter(),
    ):
        # presenters
        self._exception_presenter: _presenters.WrappedCorqOutputPresenter = (
            exception_presenter
        )
        self._config_presenter: _presenters.ConfigPresenter = config_presenter
        self._prompter: _prompts.Prompter = prompter

        # data sources
        self._config_repo: _repos.ConfigRepo = config_repo

    def on_cmd_call(self):
        """
        Call the config list command action, catching any exceptions that arise.
        """
        try:
            self._on_cmd_call_with_exceptions()
        except Exception as e:
            self._exception_presenter.show_error(e)

    def _on_cmd_call_with_exceptions(self):
        """
        Implementation of the command action. Doesn't catch exceptions.
        """
        self._config_presenter.print_configs_list(
            [
                self._config_repo.read_config(config_name)
                for config_name in self._config_repo.list_remote_config_names()
            ],
            message="Stored Logins:",
        )
