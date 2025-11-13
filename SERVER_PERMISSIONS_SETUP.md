# Server Permissions Setup Guide

## Problem
When uploading SQL zip files on the Ubuntu server, the application fails because it cannot create or write to the `temp_sql_imports` directory.

## Solution

The Django application needs write permissions to the `media` directory and its subdirectories. Based on your service file, the application runs as user `frtc` and group `frtc`.

### Step 1: Check Current Permissions

First, check the current permissions and ownership:

```bash
# Navigate to your project directory
cd /home/frtc/webapp/carbonapi

# Check if media directory exists
ls -la | grep media

# Check permissions on media directory (if it exists)
ls -ld media/
```

### Step 2: Create Media Directory (if it doesn't exist)

```bash
# Create media directory if it doesn't exist
mkdir -p /home/frtc/webapp/carbonapi/media

# Create temp_sql_imports subdirectory
mkdir -p /home/frtc/webapp/carbonapi/media/temp_sql_imports
```

### Step 3: Set Correct Ownership

Set the ownership to the `frtc` user and group (the user running your Django service):

```bash
# Set ownership of media directory to frtc user and group
sudo chown -R frtc:frtc /home/frtc/webapp/carbonapi/media

# Verify ownership
ls -ld /home/frtc/webapp/carbonapi/media
ls -ld /home/frtc/webapp/carbonapi/media/temp_sql_imports
```

### Step 4: Set Correct Permissions

Set appropriate permissions for the media directory:

```bash
# Set directory permissions (755 = owner can read/write/execute, group and others can read/execute)
sudo chmod -R 755 /home/frtc/webapp/carbonapi/media

# For better security, you can use 750 (owner and group can read/write/execute, others have no access)
# sudo chmod -R 750 /home/frtc/webapp/carbonapi/media
```

### Step 5: Ensure Parent Directory Has Correct Permissions

Make sure the parent directory also has correct permissions:

```bash
# Check and set permissions on the project root
sudo chown frtc:frtc /home/frtc/webapp/carbonapi
sudo chmod 755 /home/frtc/webapp/carbonapi
```

### Step 6: Restart Your Django Service

After setting permissions, restart your service:

```bash
# Restart the service
sudo systemctl restart frtc_analysis.service

# Check service status
sudo systemctl status frtc_analysis.service

# View service logs if there are issues
sudo journalctl -u frtc_analysis.service -f
```

## Verification

Test that the application can create directories:

```bash
# As the frtc user, test directory creation
sudo -u frtc mkdir -p /home/frtc/webapp/carbonapi/media/temp_sql_imports/test_dir
sudo -u frtc touch /home/frtc/webapp/carbonapi/media/temp_sql_imports/test_file.txt
sudo -u frtc rm -rf /home/frtc/webapp/carbonapi/media/temp_sql_imports/test_dir
sudo -u frtc rm /home/frtc/webapp/carbonapi/media/temp_sql_imports/test_file.txt
```

If these commands work without errors, permissions are set correctly.

## Alternative: Using ACL (Access Control Lists)

If you need more granular control, you can use ACL:

```bash
# Install ACL tools (if not already installed)
sudo apt-get install acl

# Set ACL to allow frtc user full access
sudo setfacl -R -m u:frtc:rwx /home/frtc/webapp/carbonapi/media
sudo setfacl -R -d -m u:frtc:rwx /home/frtc/webapp/carbonapi/media
```

## Troubleshooting

### Issue: Permission Denied Errors

If you still get permission errors:

1. **Check SELinux** (if enabled):
   ```bash
   # Check if SELinux is enabled
   getenforce
   
   # If enabled, you may need to set SELinux context
   sudo chcon -R -t httpd_sys_rw_content_t /home/frtc/webapp/carbonapi/media
   ```

2. **Check AppArmor** (if enabled):
   ```bash
   # Check AppArmor status
   sudo aa-status
   ```

3. **Check if the directory is on a separate mount**:
   ```bash
   # Check mount options
   mount | grep /home
   ```

### Issue: Directory Created But Files Cannot Be Written

If directories are created but files cannot be written:

```bash
# Ensure the directory has write permission
sudo chmod 775 /home/frtc/webapp/carbonapi/media/temp_sql_imports

# Or use sticky bit for group collaboration
sudo chmod 2775 /home/frtc/webapp/carbonapi/media/temp_sql_imports
```

## Quick Setup Script

You can run this complete setup script:

```bash
#!/bin/bash
# Run as root or with sudo

PROJECT_DIR="/home/frtc/webapp/carbonapi"
MEDIA_DIR="$PROJECT_DIR/media"
TEMP_DIR="$MEDIA_DIR/temp_sql_imports"

# Create directories
mkdir -p "$TEMP_DIR"

# Set ownership
chown -R frtc:frtc "$MEDIA_DIR"

# Set permissions
chmod -R 755 "$MEDIA_DIR"

# Verify
echo "Verifying permissions..."
ls -ld "$MEDIA_DIR"
ls -ld "$TEMP_DIR"

echo "Setup complete!"
```

Save this as `setup_permissions.sh`, make it executable, and run it:

```bash
chmod +x setup_permissions.sh
sudo ./setup_permissions.sh
```

## Notes

- The `media` directory should be owned by the user running the Django process (`frtc` in your case)
- The `temp_sql_imports` directory will be created automatically by the application, but the parent `media` directory must exist and be writable
- After setting permissions, always restart your Django service
- Consider setting up a cron job or systemd timer to periodically clean up old temp files

