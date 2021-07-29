# Push release to PyPI

# Create dist 
python3 setup.py sdist bdist_wheel

# Upload to PyPI
twine upload dist/*

# Cleanup
rm -rf build dist *.egg-info