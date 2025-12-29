echo "================================================"
cat ./Note_to_self.md

echo "================================================"

echo ""
echo "Begin rsync -av --exclude='.venv' ./ ~/gh/qlir/examples/update_proj_template_with_this/"
rsync -av --exclude='.venv' --exclude='__pycache__' ./ ~/gh/qlir/examples/update_proj_template_with_this/

echo "Copy Done - Project template copied to qlir examples"