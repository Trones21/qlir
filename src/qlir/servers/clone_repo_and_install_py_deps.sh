git clone https://github.com/Trones21/qlir.git

cd ~/qlir

rm poetry.lock
echo "Removed poetry lockfile"

poetry install

cd ~/qlir/examples/update_proj_template_with_this/

echo "Please choose a location from which the qlir package should be used (as well as editable v. non-editable)"

poetry install


