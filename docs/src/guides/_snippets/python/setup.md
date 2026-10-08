Set up your Python development environment:

:::{dropdown} Verify Python version 3.10 to 3.14

Verify your Python version number is between 3.10 to 3.14:

  ```bash
  python --version
  ```

  or

  ```bash
  python3 --version
  ```

  If your version is outside this range, update Python to a supported version. If you get an error, you might not have Python. [Download and install Python](https://www.python.org/downloads/).
:::

:::::{dropdown} Set up a virtual environment

Keep the Nominal SDK and its dependencies separate from other Python projects using a virtual environment:
1. Set up a virtual environment:

    ::::{tab-set}

    :::{tab-item} macOS/Linux

    ```bash
    python3 -m venv nominal-env
    ```
    :::

    :::{tab-item} Windows

    ```bash
    python -m venv nominal-env
    ```
    :::
    ::::

    This creates a new folder called `nominal-env` containing an isolated Python installation.

1. Activate your virtual environment:

    ::::{tab-set}

    :::{tab-item} macOS/Linux

    ```bash
    source nominal-env/bin/activate
    ```
    :::

    :::{tab-item} Windows (Command Prompt)

    ```bash
    nominal-env\Scripts\activate.bat
    ```
    :::

    :::{tab-item} Windows (PowerShell)

    ```bash
    nominal-env\Scripts\Activate.ps1
    ```
    :::
    ::::

    When activated, you should see `(nominal-env)` at the beginning of your command prompt.
:::::
