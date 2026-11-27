#!/usr/bin/env bash
# backport-pr.sh - turn one or more GitHub PRs into a backport branch on a release branch.
#
# Works for PRs from any fork: GitHub publishes every PR as refs/pull/<n>/head on the base repository, so
# nothing is fetched from the contributor's repository.
#
#   Merged PR     -> cherry-picks the merge commit (-m 1), or the squash commit, from the base branch.
#   Not merged PR -> cherry-picks the PR's own commits (merges of the base branch into the PR are skipped).
#
# Every cherry-pick uses -x, so each commit records its source. A cherry-picked merge commit gets the PR title
# as subject and the PR author as author (other PR authors as Co-authored-by).
#
# Usage:
#   backport-pr.sh [options] <target-branch> <pr> [<pr> ...]
#
# Examples:
#   backport-pr.sh v0.11.0 3303 3305
#   backport-pr.sh --changelog history/0_11_0_release/CHANGELOG.md --open-pr v0.11.0 3303 3305
#
# Options:
#   -r, --remote NAME     remote of the base repository (default: origin)
#   -B, --base NAME       branch the PRs were made against, used without gh (default: main)
#   -b, --branch NAME     name of the new branch (default: backport/<target>/pr-<n>[-<n>...])
#   -c, --changelog FILE  move the history/latest/*.md files that the PRs add into FILE, at the end of its
#                         "## Bug Fixes" section (another section: --changelog-section), as one extra commit
#       --changelog-section TITLE   section heading for --changelog (default: "Bug Fixes")
#       --push            push the branch to --push-remote (default: the --remote)
#       --push-remote NAME
#       --open-pr         push, then open a PR against <target-branch> with gh (implies --push)
#       --print-pr        print the PR title and body instead of opening the PR (no push)
#       --repo OWNER/NAME repository for gh (default: taken from the remote URL)
#       --no-gh           use git only, even when gh is installed
#       --resume          continue after a resolved conflict (see below)
#   -h, --help
#
# Needs git. gh is optional: with gh the script reads the PR state, title and merge commit from GitHub;
# without it, a PR counts as merged when the base branch has a "Merge pull request #<n> from" commit.
#
# On a conflict the script stops in the cherry-pick. Resolve it, run `git cherry-pick --continue`, then run the
# script again with the same arguments plus --resume: it skips what the branch already has.

set -euo pipefail

remote=origin
base=main
branch=
changelog=
changelog_section="Bug Fixes"
push=0
push_remote=
open_pr=0
repo=
resume=0
print_pr=0
no_gh=0
target=
prs=()
orig_args=("$@")

die() { echo "backport-pr: $*" >&2; exit 1; }
info() { echo "backport-pr: $*" >&2; }
usage() { sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

while [ $# -gt 0 ]; do
  case "$1" in
    -r|--remote) remote=$2; shift 2 ;;
    -B|--base) base=$2; shift 2 ;;
    -b|--branch) branch=$2; shift 2 ;;
    -c|--changelog) changelog=$2; shift 2 ;;
    --changelog-section) changelog_section=$2; shift 2 ;;
    --push) push=1; shift ;;
    --push-remote) push_remote=$2; shift 2 ;;
    --open-pr) open_pr=1; push=1; shift ;;
    --repo) repo=$2; shift 2 ;;
    --resume) resume=1; shift ;;
    --print-pr) print_pr=1; shift ;;
    --no-gh) no_gh=1; shift ;;
    -h|--help) usage 0 ;;
    -*) die "unknown option $1 (see --help)" ;;
    *) if [ -z "$target" ]; then target=$1; else prs+=("$1"); fi; shift ;;
  esac
done

[ -n "$target" ] && [ ${#prs[@]} -gt 0 ] || usage 1
for n in "${prs[@]}"; do
  case "$n" in ''|*[!0-9]*) die "PR number expected, got '$n'" ;; esac
done
push_remote=${push_remote:-$remote}
if [ -z "$branch" ]; then
  branch="backport/$target/pr-$(IFS=-; echo "${prs[*]}")"
fi

git rev-parse --git-dir >/dev/null 2>&1 || die "run this inside the repository"
cd "$(git rev-parse --show-toplevel)"
git remote get-url "$remote" >/dev/null 2>&1 || die "no remote '$remote'"

have_gh=0
if [ "$no_gh" = 0 ] && command -v gh >/dev/null 2>&1 && gh auth status >/dev/null 2>&1; then have_gh=1; fi
if [ -z "$repo" ]; then
  repo=$(git remote get-url "$remote" | sed -E 's#^(https://[^/]+/|git@[^:]+:|ssh://git@[^/]+/)##; s#\.git$##')
fi
if [ "$open_pr" = 1 ] && [ "$have_gh" = 0 ]; then die "--open-pr needs gh, logged in"; fi

if [ "$resume" = 0 ]; then
  [ -z "$(git status --porcelain --untracked-files=no)" ] || die "the working tree has uncommitted changes"
  if git rev-parse -q --verify "refs/heads/$branch" >/dev/null; then
    die "branch $branch exists already (delete it, choose --branch, or use --resume)"
  fi
else
  [ "$(git rev-parse --abbrev-ref HEAD)" = "$branch" ] || die "--resume: check out $branch first"
  if git rev-parse -q --verify CHERRY_PICK_HEAD >/dev/null; then
    die "--resume: a cherry-pick is still in progress; finish it with git cherry-pick --continue"
  fi
fi

# --- fetch -------------------------------------------------------------------------------------------------
refspecs=("+refs/heads/$target:refs/remotes/$remote/$target" "+refs/heads/$base:refs/remotes/$remote/$base")
for n in "${prs[@]}"; do refspecs+=("+refs/pull/$n/head:refs/backport-pr/$n"); done
info "fetching $target, $base and PR heads from $remote"
git fetch --quiet --no-tags "$remote" "${refspecs[@]}"

# --- work out what to cherry-pick for each PR ----------------------------------------------------------------
picks=()      # "<commit> <-m 1 or empty>"
titles=()
summary=""
for n in "${prs[@]}"; do
  head=$(git rev-parse "refs/backport-pr/$n")
  state=""; merge=""; title=""; pr_base=$base
  if [ "$have_gh" = 1 ]; then
    IFS=$'\t' read -r state merge pr_base title < <(gh pr view "$n" -R "$repo" \
      --json state,mergeCommit,baseRefName,title \
      --jq '[.state, (.mergeCommit.oid // "-"), .baseRefName, .title] | @tsv')
    [ "$merge" = "-" ] && merge=""
    if [ "$pr_base" != "$base" ]; then
      git fetch --quiet --no-tags "$remote" "+refs/heads/$pr_base:refs/remotes/$remote/$pr_base"
    fi
  else
    merge=$(git log --merges --format=%H -1 --grep="^Merge pull request #$n from" "$remote/$base" || true)
    if [ -n "$merge" ]; then
      state=MERGED; title=$(git log -1 --format=%b "$merge" | sed -n '/./{p;q;}')
    else
      state=OPEN
    fi
    [ -n "$title" ] || title=$(git log -1 --format=%s "$head")
  fi
  titles+=("$title")

  if [ "$state" = MERGED ] && [ -n "$merge" ]; then
    git cat-file -e "$merge^{commit}" 2>/dev/null || git fetch --quiet --no-tags "$remote" "$merge"
    parents=$(git rev-list --parents -n1 "$merge" | wc -w | tr -d ' ')
    if [ "$parents" -gt 2 ]; then
      picks+=("$merge m1 $n")
      summary+="| #$n | merged | \`$(git rev-parse --short "$merge")\` (merge commit, \`-m 1\`) |"$'\n'
    elif git log -1 --format=%s "$merge" | grep -q "(#$n)\$"; then
      picks+=("$merge - $n")
      summary+="| #$n | squash-merged | \`$(git rev-parse --short "$merge")\` |"$'\n'
    else
      # rebase merge: GitHub rewrote the PR commits; take them from the PR head
      mb=$(git merge-base "$head" "$remote/$pr_base")
      count=0
      for c in $(git rev-list --reverse --no-merges "$mb..$head"); do picks+=("$c - $n"); count=$((count + 1)); done
      summary+="| #$n | rebase-merged | PR head \`$(git rev-parse --short "$head")\`, $count commit(s) |"$'\n'
    fi
  else
    [ "$state" = CLOSED ] && info "warning: #$n is closed and not merged"
    mb=$(git merge-base "$head" "$remote/$pr_base")
    list=$(git rev-list --reverse --no-merges "$mb..$head")
    [ -n "$list" ] || die "#$n: no commits between $pr_base and the PR head"
    count=0
    for c in $list; do picks+=("$c - $n"); count=$((count + 1)); done
    summary+="| #$n | not merged | PR head \`$(git rev-parse --short "$head")\`, $count commit(s) |"$'\n'
  fi
done

# --- branch and cherry-pick --------------------------------------------------------------------------------
if [ "$resume" = 0 ]; then
  info "creating $branch from $remote/$target"
  git switch --quiet -c "$branch" --no-track "$remote/$target"
fi

already_has() {  # is commit $1 already cherry-picked onto the branch (by the -x line)?
  git log --format=%B "$remote/$target..HEAD" | grep -q "cherry picked from commit $1"
}

title_of() {  # title of PR $1
  local i=0
  for x in "${prs[@]}"; do [ "$x" = "$1" ] && { echo "${titles[$i]}"; return; }; i=$((i + 1)); done
}

# A cherry-picked merge commit has the merger as author and "Merge pull request ..." as subject.
# Give it the PR title and the PR's author(s) instead.
reword_merge_pick() {  # $1 = merge commit, $2 = PR number; HEAD is the cherry-pick of $1
  local merge=$1 n=$2 head mb author coauthors
  git log -1 --format=%s HEAD | grep -q '^Merge pull request #' || return 0
  head=$(git rev-parse "refs/backport-pr/$n")
  mb=$(git merge-base "$head" "$merge^1")
  author=$(git log --no-merges --reverse --format='%an <%ae>' "$mb..$head" | sed -n 1p)
  [ -n "$author" ] || return 0
  coauthors=$(git log --no-merges --format='%an <%ae>' "$mb..$head" | awk -v a="$author" '$0 != a && !seen[$0]++ { print "Co-authored-by: " $0 }')
  git commit --amend --quiet --no-verify --author="$author" \
    -m "$(title_of "$n") (#$n)" \
    -m "Backport of #$n (merge commit $(git rev-parse --short "$merge"))." \
    -m "(cherry picked from commit $merge)${coauthors:+
$coauthors}"
}

for p in "${picks[@]}"; do
  set -- $p
  c=$1; n=$3; opt=""
  [ "$2" = m1 ] && opt="-m 1"
  if already_has "$c"; then
    # a commit finished by hand after a conflict still needs the PR title and author
    [ "$2" = m1 ] && git log -1 --format=%B HEAD | grep -q "cherry picked from commit $c" && reword_merge_pick "$c" "$n"
    info "skip $(git rev-parse --short "$c"): already on the branch"; continue
  fi
  info "cherry-pick $(git log -1 --format='%h %s' "$c")"
  # shellcheck disable=SC2086
  if ! git cherry-pick -x $opt "$c"; then
    if git rev-parse -q --verify CHERRY_PICK_HEAD >/dev/null && [ -z "$(git diff --cached --name-only)" ] \
       && [ -z "$(git diff --name-only)" ]; then
      info "skip $(git rev-parse --short "$c"): $target has this change already (empty cherry-pick)"
      git cherry-pick --skip
      continue
    fi
    cat >&2 <<EOF

backport-pr: CONFLICT while cherry-picking $(git rev-parse --short "$c").
  Resolve the files above, then:
    git add <files> && git cherry-pick --continue
    $0 --resume $(printf '%q ' "${orig_args[@]}" | sed 's/--resume //')
  Or give up:  git cherry-pick --abort && git switch - && git branch -D $branch
EOF
    exit 2
  fi
  [ "$2" = m1 ] && reword_merge_pick "$c" "$n"
done

# --- changelog ---------------------------------------------------------------------------------------------
added=$(git diff --name-only --diff-filter=A "$remote/$target..HEAD" -- 'history/latest/*.md' || true)
if [ -n "$added" ]; then
  if [ -n "$changelog" ]; then
    [ -f "$changelog" ] || die "changelog $changelog does not exist on $target"
    tmp=$(mktemp)
    for f in $added; do cat "$f"; printf '\n'; done > "$tmp"
    awk -v section="## $changelog_section" -v entries="$tmp" '
      { line[NR] = $0 }
      END {
        s = 0
        for (i = 1; i <= NR; i++) { t = line[i]; sub(/[ \t\r]+$/, "", t); if (t == section) { s = i; break } }
        if (!s) exit 3
        e = NR + 1
        for (i = s + 1; i <= NR; i++) if (line[i] ~ /^## /) { e = i; break }
        last = e - 1
        while (last > s && line[last] ~ /^[ \t\r]*$/) last--
        for (i = 1; i <= last; i++) print line[i]
        print ""
        k = 0
        while ((getline l < entries) > 0) buf[++k] = l
        while (k > 0 && buf[k] ~ /^[ \t\r]*$/) k--
        for (i = 1; i <= k; i++) print buf[i]
        if (e <= NR) { print ""; for (i = e; i <= NR; i++) print line[i] }
      }' "$changelog" > "$changelog.tmp" || { rm -f "$changelog.tmp" "$tmp"; die "no '## $changelog_section' section in $changelog"; }
    mv "$changelog.tmp" "$changelog"
    rm -f "$tmp"
    # shellcheck disable=SC2086
    git rm --quiet $added
    git add "$changelog"
    git commit --quiet -m "Move the release notes of $(printf '#%s ' "${prs[@]}" | sed 's/ $//') to $changelog

$target has no history/latest/, so the entries go to the end of \"## $changelog_section\" in $changelog."
    info "moved $(echo "$added" | wc -l | tr -d ' ') release note(s) into $changelog"
  elif ! git ls-tree -d --name-only "$remote/$target" history/latest | grep -q .; then
    info "warning: the PRs add $(echo "$added" | tr '\n' ' ')but $target has no history/latest/."
    info "         Run again with --resume --changelog history/<release>/CHANGELOG.md, or move them by hand."
  fi
fi

echo
git log --oneline "$remote/$target..HEAD"
git diff --stat "$remote/$target..HEAD" | tail -1

# --- push and PR -------------------------------------------------------------------------------------------
if [ "$push" = 1 ] && [ "$print_pr" = 0 ]; then
  info "pushing $branch to $push_remote"
  git push --set-upstream "$push_remote" "$branch"
fi
if [ "$open_pr" = 1 ] || [ "$print_pr" = 1 ]; then
  head_ref=$branch
  push_repo=$(git remote get-url "$push_remote" | sed -E 's#^(https://[^/]+/|git@[^:]+:|ssh://git@[^/]+/)##; s#\.git$##')
  [ "$push_repo" != "$repo" ] && head_ref="${push_repo%%/*}:$branch"
  refs=$(printf '#%s, ' "${prs[@]}" | sed 's/, $//')
  title="[$target] ${titles[0]} (backport $refs)"
  body=$(cat <<EOF
## Summary
Backports $refs to \`$target\`.

| PR | State | Cherry-picked |
|---|---|---|
$summary
Each commit is cherry-picked with \`-x\` and keeps the PR author as its author. Made with \`backport-pr.sh\`.

Closing keywords do not work in a PR against \`$target\`: close the linked issues by hand after the merge.
EOF
)
  if [ "$print_pr" = 1 ]; then
    printf '\n--- PR into %s:%s from %s\n%s\n\n%s\n' "$repo" "$target" "$head_ref" "$title" "$body"
  else
    gh pr create -R "$repo" --base "$target" --head "$head_ref" --title "$title" --body "$body"
  fi
fi
