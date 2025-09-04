# SQL Import Cleanup System

This document describes the cleanup system implemented for SQL zip file imports to automatically clean up temporary directories after import operations.

## Overview

When SQL zip files are imported, they are temporarily stored in `media/temp_sql_imports/{import_id}/` directories. The cleanup system ensures these temporary files are removed after the import process completes (whether successful or failed) to prevent disk space accumulation.

## Features

### 1. Automatic Cleanup
- **On Success**: Temporary directories are automatically cleaned up after successful imports
- **On Failure**: Temporary directories are automatically cleaned up when imports fail
- **On Error**: Temporary directories are cleaned up when exceptions occur during the import process

### 2. Manual Cleanup APIs
Three new API endpoints are available for manual cleanup operations:

#### Cleanup Specific Import
```
POST /inventory/cleanup-temp/
Parameters:
- import_id: The specific import ID to clean up
```

#### Cleanup Old Directories
```
POST /inventory/cleanup-old-temp/
Parameters:
- max_age_hours: Maximum age in hours (optional, default 24)
```

#### Cleanup Failed Imports
```
POST /inventory/cleanup-failed-imports/
Parameters: None
```

### 3. Management Command
A Django management command is available for automated cleanup:

```bash
# Clean up both old and failed directories (default)
python manage.py cleanup_temp_files

# Clean up only old directories (older than 12 hours)
python manage.py cleanup_temp_files --old-only --max-age-hours 12

# Clean up only failed imports
python manage.py cleanup_temp_files --failed-only

# Clean up old directories (older than 48 hours)
python manage.py cleanup_temp_files --max-age-hours 48
```

## Implementation Details

### Cleanup Functions (utils.py)

1. **`cleanup_temp_directory(import_id)`**
   - Removes a specific temporary directory for a given import ID
   - Returns (success, message) tuple

2. **`cleanup_old_temp_directories(max_age_hours=24)`**
   - Removes temporary directories older than specified hours
   - Uses directory creation time to determine age
   - Returns (success, message) tuple

3. **`cleanup_failed_imports()`**
   - Removes temporary directories for imports marked as 'failed' in the database
   - Returns (success, message) tuple

### Integration Points

The cleanup system is integrated into the import process at these points:

1. **upload_sql_zip view**: Cleanup on exception
2. **confirm_import view**: Cleanup on success, failure, and exceptions

### File Structure

```
carbonapi/
├── inventory/
│   ├── utils.py                    # Cleanup functions
│   ├── views.py                    # API endpoints with cleanup integration
│   ├── urls.py                     # Cleanup API routes
│   └── management/
│       └── commands/
│           └── cleanup_temp_files.py  # Management command
├── test_cleanup.py                 # Test script
└── CLEANUP_README.md              # This documentation
```

## Usage Examples

### API Usage

```python
import requests

# Clean up a specific import
response = requests.post('http://localhost:8000/inventory/cleanup-temp/', {
    'import_id': 'your-import-id'
})

# Clean up old directories (older than 6 hours)
response = requests.post('http://localhost:8000/inventory/cleanup-old-temp/', {
    'max_age_hours': 6
})

# Clean up failed imports
response = requests.post('http://localhost:8000/inventory/cleanup-failed-imports/')
```

### Scheduled Cleanup

To set up automated cleanup, you can use cron jobs or task schedulers:

```bash
# Run cleanup every hour
0 * * * * cd /path/to/carbonapi && python manage.py cleanup_temp_files

# Run cleanup daily at 2 AM
0 2 * * * cd /path/to/carbonapi && python manage.py cleanup_temp_files --max-age-hours 24
```

## Testing

Run the test script to verify cleanup functionality:

```bash
cd carbonapi
python test_cleanup.py
```

## Configuration

The cleanup system uses the following Django settings:

- `MEDIA_ROOT`: Base directory for media files (default: `BASE_DIR / 'media'`)
- Temporary files are stored in: `MEDIA_ROOT / 'temp_sql_imports' / {import_id}`

## Error Handling

- All cleanup functions return (success, message) tuples
- Failed cleanup operations are logged but don't stop the import process
- Directory existence is checked before attempting removal
- Graceful handling of missing directories or files

## Security Considerations

- Only directories within the `temp_sql_imports` folder are cleaned up
- Import ID validation prevents directory traversal attacks
- Cleanup operations are logged for audit purposes

## Monitoring

Monitor cleanup operations by:

1. Checking API response messages
2. Reviewing Django logs
3. Monitoring disk space usage
4. Running the management command with verbose output

## Troubleshooting

### Common Issues

1. **Permission Errors**: Ensure the Django process has write permissions to the media directory
2. **Directory Not Found**: This is normal for already cleaned up directories
3. **Database Connection Issues**: Failed import cleanup requires database access

### Debug Mode

Enable debug logging to see detailed cleanup operations:

```python
import logging
logging.getLogger('inventory.utils').setLevel(logging.DEBUG)
```
