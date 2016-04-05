# Typing 'make' in the directory builds the tmp folder (and deletes old one)
run: delete create
	
# creates the tmp folder
create:
	mkdir /tmp/doupton_db

# removes old directory
delete:
	rm -rf /tmp/doupton_db

#Type 'make test' to run experiment
test:
	python3 experiment.py
    
