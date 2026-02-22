# Terraform State Management - Final Setup Checklist

## Completed

- [x] Updated GitHub Actions workflow with state persistence
- [x] Configured path filters to avoid unnecessary triggers
- [x] Added state download/upload steps to both jobs
- [x] Created comprehensive documentation
- [x] Terraform main.tf already using local backend

## Next Steps (Do These Now)

### 1. Remove State Files from Git
```bash
cd "d:\Documents\Programming Projects\snowflake-e2e-project"

# Remove state files from Git tracking (keeps local files)
git rm --cached terraform/terraform.tfstate
git rm --cached terraform/terraform.tfstate.backup

# Verify .gitignore includes state files
cat .gitignore | grep -A 5 "# Terraform"
```

**Expected in .gitignore:**
```
# Terraform
terraform/.terraform/
terraform/*.tfstate
terraform/*.tfstate.*
terraform/*.tfplan
```

### 2. Commit and Push Changes
```bash
git status  # Should show modified workflow and removed state files

git add .github/workflows/deploy-snowflake-infraestructure.yaml
git add .github/STATE-MANAGEMENT-UPDATE.md
git add terraform/STATE-MANAGEMENT.md

git commit -m "feat: implement GitHub Artifacts state management

- Add state download/upload to workflow
- Configure path filters for Terraform files
- Remove state files from version control
- Add comprehensive state management documentation"

git push origin main
```

### 3. Monitor First Workflow Run
1. Go to GitHub → **Actions** tab
2. Watch **Deploy Snowflake Infrastructure** workflow
3. Verify these steps succeed:
   - "Download previous Terraform state" (warning expected on first run)
   - "Terraform Init"
   - "Terraform Plan"
   - "Upload Terraform state (from plan)"
   - "Terraform Apply" (if on main branch)
   - "Upload Terraform state"

### 4. Verify State Artifact Created
1. Click on the completed workflow run
2. Scroll down to **Artifacts** section
3. Confirm presence of:
   - `terraform-plan` (the execution plan)
   - `terraform-state` (the state file) ← **Most important**

### 5. Test State Persistence (Optional but Recommended)
Make a trivial change to verify state is working:

```hcl
# terraform/main.tf - line 26
resource "snowflake_database" "main" {
  name    = var.database_name
  comment = "Air quality data pipeline ${var.environment} database - State test!" # Change this
}
```

```bash
git add terraform/main.tf
git commit -m "test: verify state persistence"
git push origin main
```

**Expected behavior:**
- Workflow downloads previous state
- Plan shows ONLY the comment change (not recreating database)
- Apply updates just the comment
- No resources destroyed/recreated

### 6. Clean Up Test Change
```hcl
# terraform/main.tf - revert to original
resource "snowflake_database" "main" {
  name    = var.database_name
  comment = "Air quality data pipeline ${var.environment} database"
}
```

```bash
git add terraform/main.tf
git commit -m "revert: remove test comment"
git push origin main
```

## Success Criteria

You'll know everything is working when:

- State files no longer in Git history (new commits)
- GitHub Actions workflow completes successfully
- `terraform-state` artifact appears after each run
- Subsequent runs download and use previous state
- Terraform plans show incremental changes (not full recreates)
- No "resources will be destroyed" unless intentional

## Common Issues

### Issue 1: Git Remove Failed
```bash
# Error: pathspec 'terraform/terraform.tfstate' did not match any files
```
**Cause:** File not in Git index (might already be removed)  
**Solution:** Check with `git status` - if file isn't shown as tracked, you're good!

### Issue 2: Workflow Triggers on State File Commit
**Cause:** Pushing state file changes  
**Solution:** Already fixed! Path filters ignore `*.tfstate` files

### Issue 3: State Download Warns on First Run
```
Warning: Unable to download artifact: no artifact found
```
**Cause:** First workflow run has no previous state  
**Solution:** This is expected! Check that state uploads successfully after run

### Issue 4: Resources Being Recreated
**Symptom:** Terraform wants to destroy and recreate everything  
**Cause:** State file wasn't downloaded or is missing  
**Solution:** 
1. Check workflow logs - did state download succeed?
2. Check Artifacts section - is `terraform-state` present?
3. If missing, workflow may need to run once to create initial state

## Documentation Reference

- [.github/STATE-MANAGEMENT-UPDATE.md](.github/STATE-MANAGEMENT-UPDATE.md) - What changed and why
- [terraform/STATE-MANAGEMENT.md](terraform/STATE-MANAGEMENT.md) - Detailed state management guide
- [.github/workflows/deploy-snowflake-infraestructure.yaml](.github/workflows/deploy-snowflake-infraestructure.yaml) - Updated workflow

## After Setup

Once everything is working:

1. **Use GitHub Actions for all infrastructure changes**
   - Don't run `terraform apply` locally
   - Let CI/CD manage state
   - Review plans in pull requests

2. **Monitor state artifacts**
   - Check artifacts exist after each run
   - Download if you need to debug state issues
   - Understand the 90-day retention period

3. **Consider future upgrades**
   - For team collaboration: Migrate to Terraform Cloud
   - For production: Consider S3 + DynamoDB with state locking
   - For now: This setup is perfect for learning!

## Need Help?

If something goes wrong:
1. Check GitHub Actions logs for specific errors
2. Review [terraform/STATE-MANAGEMENT.md](terraform/STATE-MANAGEMENT.md)
3. Verify [.github/workflows/deploy-snowflake-infraestructure.yaml](.github/workflows/deploy-snowflake-infraestructure.yaml) matches the updated version
4. Ensure Snowflake credentials are properly configured in GitHub Secrets

## You're Done When...

All these commands succeed:
```bash
# No state files in Git
git ls-files | grep terraform.tfstate
# Expected: (no output)

# Workflow file has state management
grep -A 3 "Download previous Terraform state" .github/workflows/deploy-snowflake-infraestructure.yaml
# Expected: Shows the state download step

# Latest workflow run has artifacts
# Check manually: GitHub → Actions → Latest run → Artifacts section
# Expected: terraform-state artifact present
```

---

**Ready to go! Follow the checklist above and you'll have state management fully operational.**
