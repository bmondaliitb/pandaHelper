A package using panda API to help manage the panda jobs

to download tty_dilepton jobs:
```
python tty_dilepton_manage.py --job_output_filename output_ttgamma --output_path /afs/cern.ch/work/b/bmondal/PandaAPI/test_path
```

To create broken list samples:
```python helpmePanda.py --output_filename output_ttgamma```
This will create `broken_samples/MC16a_TOPQ1.py, broken_samples/MC16d_TOPQ1.py, broken_samples/MC16e_TOPQ1.py` files.

copy this files to the `ttgamma-ntuple-production/TTGammaEventSaver/python/` folder

then run tty-grid-submit.py the way you use `tty-grid-submit.py -typ reco -camp a -ch dilep_CR -submit -trial 0`

