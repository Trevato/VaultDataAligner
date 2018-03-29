# VaultDataAligner

Align Vault Dairy data in order to apply AWS Machine Learning.

## Align Data without Multi-Threading:

* Run ```spreadsheet.py``` (Python 2.7.+ and Python 3)

## Align Data with Multi-Threading (_Much faster_):

* Run ```runVaultAligner.py``` (Python 3 **only**)

_**Note:** Due to Google API throttling, errors my be produced. They can be ignored currently but working on a fix._

_**Note2:**_ ```runVaultAligner.py``` _is not necessary in the current state but is required if I change from Multi-Threading to Multi-Processing (doesn't really make sense though since the program is slow due to IO wait times)._
