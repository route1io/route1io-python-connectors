# Author: Chris Greening
# Date: 07/29/2021
# Purpose: Upload the package to PyPI

# Push release to PyPI

if [[ $1 == "" ]]
then
    echo "Please provide a Semantic version bump parameter i.e. major, minor, or patch"
fi

# Increse Semantic version number
bump2version $1

# Create dist 
python3 setup.py sdist bdist_wheel

# Upload to PyPI
twine upload dist/*

# Cleanup
rm -rf build dist *.egg-info