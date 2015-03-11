""" Module handling the snapshots """
import logging
import datetime

from boto.exception import EC2ResponseError

from automated_ebs_snapshots import volume_manager
from automated_ebs_snapshots.valid_intervals import VALID_INTERVALS

logger = logging.getLogger(__name__)


def run(connection):
    """ Ensure that we have snapshots for a given volume

    :type connection: boto.ec2.connection.EC2Connection
    :param connection: EC2 connection object
    :returns: None
    """
    volumes = volume_manager.get_watched_volumes(connection)

    for volume in volumes:
        _ensure_snapshot(connection, volume)
        _remove_old_snapshots(connection, volume)


def _create_snapshot(volume, rule):
    """ Create a new snapshot

    :type volume: boto.ec2.volume.Volume
    :param volume: Volume to snapshot
    :returns: boto.ec2.snapshot.Snapshot -- The new snapshot
    """
    logger.info('Creating new snapshot for {} based on {}'.format(volume.id, rule))
    snapshot = volume.create_snapshot(
        description="Automatic snapshot by Automated EBS Snapshots")
    # Tag backup rule for the snapshot
    snapshot.add_tag('AutomatedEBSSnapshots', rule)
    logger.info('Created snapshot {} for volume {}'.format(
        snapshot.id, volume.id))

    return snapshot


def _ensure_snapshot_for_rule(connection, volume, rule):

    interval, retention = rule.split(':')
    if interval not in VALID_INTERVALS:
        logger.warning(
            '"{}" is not a valid snapshotting interval for volume {}'.format(
                interval, volume.id))
        return

    snapshots = connection.get_all_snapshots(filters={'volume-id': volume.id,
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
            'Missing tag AutomatedEBSSnapshots for volume {}'.format(
                volume.id))
        return

    if volume.tags['AutomatedEBSSnapshots'] not in VALID_INTERVALS:
        rules = volume.tags['AutomatedEBSSnapshots']
        for rule in rules.split(','):
            _ensure_snapshot_for_rule(connection, volume, rule)
    else:
        interval = volume.tags['AutomatedEBSSnapshots']
        retention = volume.tags['AutomatedEBSSnapshotsRetention']
        _ensure_snapshot_for_rule(connection, volume,
                                  '%s:%s' % (interval, retention))


def _remove_old_snapshots_for_rule(connection, volume, rule):
    interval, retention = rule.split(':')

    # Filter snapshots with the volume id and rule
    snapshots = connection.get_all_snapshots(filters={'volume-id': volume.id,
        'tag:AutomatedEBSSnapshots': rule})

    # Sort the list based on the start time
    snapshots.sort(key=lambda x: x.start_time)

    # Remove snapshots we want to keep
    snapshots = snapshots[:-int(retention)]

    if not snapshots:
        logger.info('No old snapshots to remove')
        return

    for snapshot in snapshots:
        logger.info('Deleting snapshot {}'.format(snapshot.id))
        try:
            snapshot.delete()
        except EC2ResponseError as error:
            logger.warning('Could not remove snapshot: {}'.format(
                error.message))

    logger.info('Done deleting snapshots')


def _remove_old_snapshots(connection, volume):

    if 'AutomatedEBSSnapshots' not in volume.tags:
        logger.warning(
            'Missing tag AutomatedEBSSnapshots for volume {}'.format(
                volume.id))
        return

    if volume.tags['AutomatedEBSSnapshots'] not in VALID_INTERVALS:
        rules = volume.tags['AutomatedEBSSnapshots']
        for rule in rules.split(','):
            _remove_old_snapshots_for_rule(connection, volume, rule)
    else:
        interval = volume.tags['AutomatedEBSSnapshots']
        retention = volume.tags['AutomatedEBSSnapshotsRetention']
        _remove_old_snapshots_for_rule(connection, volume,
                                  '%s:%s' % (interval, retention))
