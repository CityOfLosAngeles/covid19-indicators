.PHONY: pip conda install

conda: conda-requirements.txt
	conda install -c conda-forge --yes --file conda-requirements.txt

pip: requirements.txt
	pip install -r requirements.txt
	pip install -e .

install: conda pip

daily_report: 
	python /app/report.py
	git add "./coronavirus-stats.pdf"
	git commit -m "Upload pdf"
	git push origin master