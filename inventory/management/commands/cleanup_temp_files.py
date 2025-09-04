from django.core.management.base import BaseCommand
from inventory.utils import cleanup_old_temp_directories, cleanup_failed_imports


class Command(BaseCommand):
    help = 'Clean up temporary SQL import directories'

    def add_arguments(self, parser):
        parser.add_argument(
            '--max-age-hours',
            type=int,
            default=24,
            help='Maximum age in hours for temp directories (default: 24)'
        )
        parser.add_argument(
            '--failed-only',
            action='store_true',
            help='Only clean up directories for failed imports'
        )
        parser.add_argument(
            '--old-only',
            action='store_true',
            help='Only clean up old directories (based on age)'
        )

    def handle(self, *args, **options):
        max_age_hours = options['max_age_hours']
        failed_only = options['failed_only']
        old_only = options['old_only']

        self.stdout.write('Starting cleanup of temporary SQL import directories...')

        if failed_only:
            # Clean up only failed imports
            success, message = cleanup_failed_imports()
            if success:
                self.stdout.write(
                    self.style.SUCCESS(f'✓ {message}')
                )
            else:
                self.stdout.write(
                    self.style.ERROR(f'✗ {message}')
                )
        elif old_only:
            # Clean up only old directories
            success, message = cleanup_old_temp_directories(max_age_hours)
            if success:
                self.stdout.write(
                    self.style.SUCCESS(f'✓ {message}')
                )
            else:
                self.stdout.write(
                    self.style.ERROR(f'✗ {message}')
                )
        else:
            # Clean up both old and failed directories
            self.stdout.write('Cleaning up old directories...')
            success1, message1 = cleanup_old_temp_directories(max_age_hours)
            if success1:
                self.stdout.write(
                    self.style.SUCCESS(f'✓ {message1}')
                )
            else:
                self.stdout.write(
                    self.style.ERROR(f'✗ {message1}')
                )

            self.stdout.write('Cleaning up failed imports...')
            success2, message2 = cleanup_failed_imports()
            if success2:
                self.stdout.write(
                    self.style.SUCCESS(f'✓ {message2}')
                )
            else:
                self.stdout.write(
                    self.style.ERROR(f'✗ {message2}')
                )

        self.stdout.write(
            self.style.SUCCESS('Cleanup process completed!')
        )
