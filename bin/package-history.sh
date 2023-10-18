#!/usr/bin/env bash

set -ex

function prepend_readme {
  if [ -f README ]
  then
    readme_fn=README
  elif [ -f README.rst ]; then
    readme_fn=README.rst
  elif [ -f README.md ]; then
    readme_fn=README.md
  else
    echo "README not found!" 1>&2
    exit 1
  fi
  echo -n "This is a copy of the py2neo package to restore the version history that got deleted.
It's not possible to re-upload a deleted version of a package to PyPI.
So if you rely on, for example, \`py2neo ~= 4.1.0\` in your project, you can simply switch to \`py2neo-history ~= 4.1.0\`.
If your project works with py2neo 2021.2.3 or above, you can keep using \`py2neo\` as usual.

Note that this project will not get any updates past version 2021.2.3.

" | cat - $readme_fn > README.tmp && mv README.tmp $readme_fn
}

function check_install {
  python3.8 -m venv venv_vendor
  . venv_vendor/bin/activate
  pip install .
  deactivate
  rm -r venv_vendor
}

function check_sed_changes() {
  if [ ! -f sed_changes.tmp ] || [ ! -s sed_changes.tmp ]
  then
    rm sed_changes.tmp
    echo "sed_changes.tmp is empty!" 1>&2
    exit 1
  fi
  rm sed_changes.tmp
}

function vendor_packages {
  if grep -q -e 'pansi' setup.py
  then
    if ! grep -q -e 'pansi>=2020.7.3' setup.py
    then
      echo "found unexpected version of pansi" 1>&2
      exit 1
    fi
    sed -i "s/pansi>=2020.7.3/pansi==2020.7.3/" setup.py
    if ! grep -q -e 'six' setup.py
    then
      echo "pansi 2020.7.3 requires six, but py2neo doesn't, so vendoring requires extra steps" 1>&2
      exit 1
    fi
    rm -rf vendor_dist
    mkdir vendor_dist
    python3.8 -m venv venv_vendor
    . venv_vendor/bin/activate
    pip install .

    #sed -i "s/\(from os import .*\)/\1, getcwd/w sed_changes.tmp" setup.py
    #check_sed_changes
    #sed -i 's/\("packages": *find_packages(.*)\),/\1 + ["vendor_dist"],/w sed_changes.tmp' setup.py
    #check_sed_changes
    #sed -i 's/\( *\)\("package_data": *{\)/\1\2\n\1    "vendor_dist": ["*"],/w sed_changes.tmp' setup.py
    #check_sed_changes

    for dep in pansi
    do
      path=$(python -c "import $dep; print($dep.__path__[0])")
      #if path=$(python -c "import $dep; print($dep.__path__[0])")
      #if version=$(python -c "from importlib.metadata import version; print(version('$dep'))")
      #then
        #url=$(curl -s "https://pypi.org/pypi/$dep/json" | jq -r '.releases."'"$version"'" | map(select(.packagetype == "sdist"))[0].url')
        #fn=$(echo "$url" | rev | cut -d/ -f1 | rev)
        #curl "$url" -o "vendor_dist/$fn"
        #if [ ! -f "vendor_dist/$fn" ] || [ ! -s "vendor_dist/$fn" ]
        #then
        #  echo "downloaded file missing!" 1>&2
        #  exit 1
        #fi
        #
        #sed -i "s|\"$dep.*\",|\"$dep @ file://localhost/{}/vendor_dist/$fn\".format(getcwd()),|w sed_changes.tmp" setup.py
        #check_sed_changes
        #
        #dep_name_count=$(grep -r --include "*.py" $dep py2neo | wc -l)
        #dep_import_count=$(grep -r --include "*.py" -E "from +$dep.* +import " py2neo | wc -l)
        #if [ "$dep_name_count" -ne "$dep_import_count" ]
        #then
        #  grep -r --include "*.py" $dep py2neo
        #  grep -r --include "*.py" -E "from +$dep.* +import " py2neo
        #  echo "dep_name_count = $dep_name_count != $dep_import_count = dep_import_count" 1>&2
        #  exit 1
        #fi
        # vendor with relative imports
        # while IFS= read -r -d '' fn
        # do
        #   depth=$(echo "$fn" | grep -o "/" | wc -l)
        #   dots=$(for (( i = 0; i < "$depth"; ++i )); do echo -n "."; done)
        #   sed -i "s/\(from \+\)$dep\(.*import \+\)/\1${dots}_${dep}\2/g" "$fn"
        # done <   <(find py2neo -type f -name '*.py' -print0)
        # vendor with absolute imports
        rm -rf py2neo/vendor/$dep
        mkdir -p py2neo/vendor
        touch py2neo/vendor/__init__.py
        cp -r $path py2neo/vendor/$dep
        find py2neo -type f -name '*.py' -print0 | xargs -0 sed -i "s/\(from \+\)$dep\(.*import \+\)/\1py2neo.vendor.$dep\2/g"
        grep -r --include "*.py" "$dep" py2neo
        echo "manually check this grep output!"
        sed -i "/\"$dep/d" setup.py
      #fi
    done
    deactivate
    rm -r venv_vendor
  fi
}

# releases with version and package hard-coded py2neo/__init__.py
for tag in release/1.6.2 release/1.6.3 py2neo-2.0 py2neo-2.0.1 py2neo-2.0.2 py2neo-2.0.3 py2neo-2.0.4 py2neo-2.0.5 py2neo-2.0.6 py2neo-2.0.7 py2neo-2.0.8 py2neo-2.0.9 py2neo-3.0.0 py2neo-3.1.0 py2neo-3.1.1 py2neo-3.1.2
do
  export PATCHED_VERSION=$tag
  git checkout $tag
  prepend_readme
  check_install
  sed -i "s/\"name\": .*,/\"name\": \"py2neo-history\",/" setup.py
  sed -i "s/name=.*,/name=\"py2neo-history\",/" setup.py
  python setup.py sdist
  git checkout -- .
done

# releases with version and package hard-coded py2neo/meta.py
for tag in py2neo-4.0.0b1 py2neo-4.0.0b2 py2neo-4.0.0 py2neo-4.1.0 py2neo-4.1.1 py2neo-4.1.2 py2neo-4.1.3 py2neo-4.3.0
do
  export PATCHED_VERSION=$tag
  git checkout $tag
  prepend_readme
  check_install
  sed -i "s/\"name\": .*,/\"name\": \"py2neo-history\",/" setup.py
  python setup.py sdist
  git checkout -- py2neo/meta.py
  git checkout -- .
done

# releases with dev version (ending in .dev0) and package hard-coded py2neo/__init__.py
for tag in 5.0b2 5.0b3 5.0b4 5.0b5 2020.7b6
do
  export PATCHED_VERSION=$tag
  git checkout $tag
  prepend_readme
  check_install
  sed -i "s/\"name\": .*,/\"name\": \"py2neo-history\",/" setup.py
  sed -i "s/__version__ = .*/__version__ = \"$tag\"/" py2neo/__init__.py
  python setup.py sdist
  git checkout -- .
done

# releases with dummy version loaded from VERSION file
for tag in 2020.0b9 2020.0rc1 2020.0.0 2020.1a1 2020.1.0 2020.1.1 2020.7b7 2020.7b8 2021.0.0 2021.0.1 2021.1.0 2021.1.1 2021.1.2 2021.1.3 2021.1.4 2021.1.5 2021.2.0 2021.2.1 2021.2.2 2021.2.3
do
  export PATCHED_VERSION=$tag
  git checkout $tag
  vendor_packages
  prepend_readme
  sed -i "s/PACKAGE_NAME = .*/PACKAGE_NAME = \"py2neo-history\"/" py2neo/meta.py
  echo -n $tag > py2neo/VERSION
  python setup.py sdist
  git checkout -- .
done

# now call
#twine upload dist/py2neo-history-*.tar.gz

# clean-up for development
#git checkout -- .; rm -r dist sed_changes.tmp vendor_dist interchange py2neo/interchange py2neo/_interchange py2neo/vendor/interchange pansi py2neo/pansi py2neo/_pansi py2neo/vendor/pansi; rm -r venv_vendor/
# test sdist for development
#deactivate; rm -r venv_tmp/; virtualenv -p 35 venv_tmp; source venv_tmp/bin/activate.fish; pip install dist/py2neo-history-2020.0b9.tar.gz
