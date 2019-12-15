from typing import Dict, List, Union

try:
    import pandas as pd
    no_pandas = False
except ImportError:
    no_pandas = True

# DocumentBundle includes pandas Series and DataFrame when pandas is imported
if no_pandas:
    DocumentBundle = Union[Dict, List[Dict]]
else:
    DocumentBundle = Union[Dict, List[Dict], pd.Series, pd.DataFrame]  # type: ignore
