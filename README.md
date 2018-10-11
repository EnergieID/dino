# Dino
## Python API client for Dino Energy Monitor

### Installation
`python3 -m pip install dino`

### Usage

```python
import pandas as pd

start = pd.Timestamp('20181001', tz='Europe/Rome')
end = pd.Timestamp('20181009', tz='Europe/Rome')
```

#### Get raw data from JSON as a Python dictionary
```python
from dino import RawDinoClient

client = RawDinoClient(client_id, client_secret, username, serial)

client.get_data(start, end)
```

#### Get data as Pandas DataFrame
```python
from dino import PandasDinoClient

client = PandasDinoClient(client_id, client_secret, username, serial)

client.get_data(start, end)

# filter specific columns
client.get_data(start, end, columns=['E0', 'E1'])
```
