from nominal.cli import nom
import warnings

if __name__ == "__main__":
    warnings.warn("`python -m nominal` is deprecated and will be removed in a future version. Use `python -m nominal.cli` instead.")
    nom()
