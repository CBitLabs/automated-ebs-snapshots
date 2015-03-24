""" Module handling the snapshots """
import logging
import datetime
import time

from boto.exception import EC2ResponseError

from automated_ebs_snapshots import volume_manager
from automated_ebs_snapshots.valid_intervals import VALID_INTERVALS

logger = logging.getLogger(__name__)


def delay():
    # There is too much volumes need to backup, sleep 1 second to avoid
    # sending too much request to AWS
    time.sleep(1)


def run(connection):
    """ Ensure that we have snapshots for a given volume

    :type connection: boto.ec2.connection.EC2Connection
    :param connection: EC2 connection object
    :returns: None
    """
    volumes = volume_manager.get_watched_volumes(connection)

    # Delete orphan snapshots that were created one week ago
    delete_orphan_snapshots(connection, volumes, 3600*24*7)

    for volume in volumes:
        _ensure_snapshot(connection, volume)
        delay()
        _remove_old_snapshots(connection, volume)
        delay()


def delete_orphan_snapshots(connection, volumes, min_delta):
    """
    Orphan snapshots whose volumes and instances were killed
    """

    def check_time_delta(time):
        timestamp = datetime.datetime.strptime(
            time,
            '%Y-%m-%dT%H:%M:%S.000Z')
        # time delta in seconds
        delta_seconds = int(
            (datetime.datetime.utcnow() - timestamp).total_seconds())
        return delta_seconds > min_delta

    # All ids of watching volumes
    watchlist = map(lambda vol: vol.id, volumes)

    # All automated-ebs-snapshots generated snapshots
    snapshots = connection.get_all_snapshots(filters={
        'tag-key': 'AutomatedEBSSnapshots'
        })

    # Get all snapshots whose volumes are not in watch list
    orphan_snapshots = filter(
        lambda x: x.volume_id not in watchlist, snapshots)
    logger.info('Found %d orphan snapshots.' %
        len(orphan_snapshots))

    # Get orphan_snapshots that were created TIME_DELTA ago
    old_orphan_snapshots = filter(
        lambda x: check_time_delta(x.start_time), orphan_snapshots)
    logger.info('Found %d orphan snapshots were created %s seconds ago.' %
        (len(old_orphan_snapshots), min_delta))

    # Delete old orphan snapshots
    for snap in old_orphan_snapshots:
        snap.delete()
        logger.info('Delete orphan snapshot %s' % snap.id)
        delay()


def _create_snapshot(volume, rule):
    """ Create a new snapshot

    :type volume: boto.ec2.volume.Volume
    :param volume: Volume to snapshot
    :returns: boto.ec2.snapshot.Snapshot -- The new snapshot
    """
    logger.info('Creating new snapshot for %s based on %s' % (volume.id, rule))
    try:
        snapshot = volume.create_snapshot(
            description="Automatic snapshot by Automated EBS Snapshots")
        # Tag backup rule for the snapshot
        snapshot.add_tag('AutomatedEBSSnapshots', rule)
    except EC2ResponseError as e:
        logger.error('Failed to create snapshot for %s due to %s' %
            (volume.id, e.error_message))
        return None

    logger.info('Created snapshot %s for volume %s' % (snapshot.id, volume.id))

    return snapshot


def _ensure_snapshot_for_rule(connection, volume, rule):

    interval, retention = rule.split(':')
    if interval not in VALID_INTERVALS:
        logger.warning(
            '"{}" is not a valid snapshotting interval for volume {}'.format(
                interval, volume.id))
        return

    snapshots = connection.get_all_snapshots(filters={
        'volume-id': volume.id,
        'tag:AutomatedEBSSnapshots': rule})

    # Create a snapshot if we don't have any
    if not snapshots:
        _create_snapshot(volume, rule)
        return

    min_delta = 3600*24*365*10  # 10 years :)
    for snapshot in snapshots:
        timestamp = datetime.datetime.strptime(
            snapshot.start_time,
            '%Y-%m-%dT%H:%M:%S.000Z')
        delta_seconds = int(
            (datetime.datetime.utcnow() - timestamp).total_seconds())

        if delta_seconds < min_delta:
            min_delta = delta_seconds

    logger.info('The newest snapshot for {} is {} seconds old'.format(
        volume.id, min_delta))
    if interval == 'hourly' and min_delta > 3600:
        _create_snapshot(volume, rule)
    elif interval == 'daily' and min_delta > 3600*24:
        _create_snapshot(volume, rule)
    elif interval == 'weekly' and min_delta > 3600*24*7:
        _create_snapshot(volume, rule)
    elif interval == 'monthly' and min_delta > 3600*24*30:
        _create_snapshot(volume, rule)
    elif interval == 'yearly' and min_delta > 3600*24*365:
        _create_snapshot(volume, rule)
    else:
        logger.info('No need for a new snapshot of {}'.format(volume.id))


def _ensure_snapshot(connection, volume):
    """ Ensure that a given volume has an appropriate snapshot

    :type connection: boto.ec2.connection.EC2Connection
    :param connection: EC2 connection object
    :type volume: boto.ec2.volume.Volume
    :param volume: Volume to check
    :returns: None
    """
    if 'AutomatedEBSSnapshots' not in volume.tags:
        logger.warning(
            'Missing tag AutomatedEBSSnapshots for volume %s' % volume.id)
        return

    if volume.tags['AutomatedEBSSnapshots'] in VALID_INTERVALS:
        # Original tags
        interval = volume.tags['AutomatedEBSSnapshots']
        retention = volume.tags['AutomatedEBSSnapshotsRetention']
        # Build a dump rule for calling _remove_old_snapshots_for_rule
        pseudo_rule = '%s:%s' % (interval, retention)
        _ensure_snapshot_for_rule(connection, volume, pseudo_rule)
    else:
        # Extended backup rules
        rules = volume.tags['AutomatedEBSSnapshots']
        for rule in rules.split(','):
            _ensure_snapshot_for_rule(connection, volume, rule)
            delay()


def _remove_old_snapshots_for_rule(connection, volume, rule):
    interval, retention = rule.split(':')

    # Filter snapshots with the volume id and rule
    snapshots = connection.get_all_snapshots(filters={
        'volume-id': volume.id,
        'tag:AutomatedEBSSnapshots': rule})

    # Sort the list based on the start time
    snapshots.sort(key=lambda x: x.start_time)

    # Remove snapshots we want to keep
    snapshots = snapshots[:-int(retention)]

    if not snapshots:
        logger.info('No old snapshots to remove')
        return

    for snapshot in snapshots:
        logger.info('Deleting snapshot %s based on %s' % (snapshot.id, rule))
        try:
            snapshot.delete()
        except EC2ResponseError as error:
            logger.warning('Could not remove snapshot: %s' % error.message)

    logger.info('Done deleting snapshots')


def _remove_old_snapshots(connection, volume):

    if 'AutomatedEBSSnapshots' not in volume.tags:
        logger.warning(
            'Missing tag AutomatedEBSSnapshots for volume %s' % volume.id)
        return

    if volume.tags['AutomatedEBSSnapshots'] in VALID_INTERVALS:
        # Original tags
        interval = volume.tags['AutomatedEBSSnapshots']
        retention = volume.tags['AutomatedEBSSnapshotsRetention']
        # Build a dump rule for calling _remove_old_snapshots_for_rule
        pseudo_rule = '%s:%s' % (interval, retention)
        _remove_old_snapshots_for_rule(connection, volume, pseudo_rule)
    else:
        # Extended backup rules
        rules = volume.tags['AutomatedEBSSnapshots']
        for rule in rules.split(','):
            _remove_old_snapshots_for_rule(connection, volume, rule)
            delay()
