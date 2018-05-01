#!/bin/bash
# python -c "from twcom.makeindex import insraw; insraw()"
python -m proc_data.fix_badname
python -m proc_data.study_com
python -m proc_data.fix_bossnode
