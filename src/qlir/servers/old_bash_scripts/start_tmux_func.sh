start_tmux() {
  local name="$1"
  log_cmd "$@"
  
  if tmux has-session -t "$name" 2>/dev/null; then
    echo "[WARN] tmux session '$name' already exists"
    return 0
  fi
    echo 
  tmux new-session -d -s "[$@]" > logs/"$name"_cmd.log 

  echo "[OK] started $name → tmux attach -t $name"
}


log_cmd() {
  printf '[CMD]'
  printf ' %q' "$@"
  printf '\n'
}