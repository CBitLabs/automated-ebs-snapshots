# -*- coding: utf-8 -*-
import logging
import re
import yaml

from boto.exception import EC2ResponseError

from automated_ebs_snapshots import volume_manager

logger = logging.getLogger(__name__)


def delay():
    # There is too much volumes need to backup, sleep 0.5 second to avoid
    # sending too much request to AWS
    time.sleep(0.5)


def load_subnet_config(file_name):
    """
    Load config file: backup rules for each subnet
    """
    with open(file_name) as f:
        config = yaml.load(f.read())
    return config


def get_instance_by_id(connection, instance_id):
    """
    Return instance object with the given instance id
    """
    reservation = connection.get_all_instances(instance_ids=[instance_id])
    return reservation[0].instances[0]


def subnet_of_volume(connection, volume):
    """
    Return subnet of the instance that attaches the given volume
    """
    instance_id = volume.attach_data.instance_id
    inst = get_instance_by_id(connection, instance_id)
    if 'subnet' in inst.tags:
        # Instance has subnet tag
        return inst.tags['subnet']
    elif 'Name' in inst.tags:
        # Parse instance name(VPC.SUBNET:HOST)
        name = inst.tags['Name']
        name_patten = re.compile('[0-9a-z]+\.([0-9a-z]+):[0-9a-z]+')
        if name_patten.match(name):
            return name_patten.match(name).group(1)
    else:
        return None


def tag_rule_for_volume(connection, volume_id, rule):
    """
    Add backup rule for the given volume by adding a tag
    """
    try:
        volume = connection.get_all_volumes(volume_ids=[volume_id])[0]
    except EC2ResponseError:
        logger.warning('Volume %s not found' % volume_id)
        return False

    # Tag is in such format: interval1:retention1,interval2:retention2
    tag = ','.join(['%s:%s' % (k, v) for k, v in rule.items()])
    volume.add_tag('AutomatedEBSSnapshots', value=tag)

    logger.info('Updated the backup rule %s to for %s' % (tag, volume_id))
    return True


def add_backup_rules(connection, config_file):
    """
    Read backup rules from config_file and add rules for all volumes
    """
    # Load backup policies from yaml file
    config = load_subnet_config(config_file)
    # For all in-use volumes
    volumes = connection.get_all_volumes(filters={'status': 'in-use'})
    for vol in volumes:
        if config['blacklist'] and vol.id in config['blacklist']:
            # Skip if the volume is in blacklist
            continue
        if not vol.attach_data:
            # Skip if the volume is not attached to any instance
            continue
        if config['exceptions'] and vol.id in config['exceptions']:
            # volume is in exception list
            tag_rule_for_volume(connection, vol.id,
                                config['exceptions'][vol.id])
        else:
            # Use subnet rules
            subnet = subnet_of_volume(connection, vol)
            if not subnet:
                # We cannot recognize its subnet
                continue
                # We don't want to backup its subnet
            if subnet not in config['rules']:
                continue
            # Add tag for vol
            tag_rule_for_volume(connection, vol.id, config['rules'][subnet])
        delay()


def remove_backup_rules(connection):
    """
    Remove backup rules for all volumes
    """
    watched_volumes = volume_manager.get_watched_volumes(connection)
    for vol in watched_volumes:
        # Removed AutomatedEBSSnapshots tag
        vol.remove_tag('AutomatedEBSSnapshots')
        logger.info('Removed %s from the watchlist' % vol.id)
        delay()


def list_buckup_rules(connection):
    """
    List backup rules for watched EBS volumes
    """
    volumes = volume_manager.get_watched_volumes(connection)

    if not volumes:
        logger.info('No watched volumes found')
        return

    logger.info(
        '+----------------'
        '+----------------------'
        '+----------------------+')
    logger.info(
        '| {volume:<14} '
        '| {volume_name:<20.20} '
        '| {rules:<20} |'.format(
            volume='Volume ID',
            volume_name='Volume name',
            rules='Backup rules'))
    logger.info(
        '+----------------'
        '+----------------------'
        '+----------------------+')

    for volume in volumes:
        if 'AutomatedEBSSnapshots' not in volume.tags:
            rules = 'Interval tag not found'
        else:
            rules = volume.tags['AutomatedEBSSnapshots']

        # Get the volume name
        try:
            volume_name = volume.tags['Name']
        except KeyError:
            volume_name = ''

        logger.info(
            '| {volume_id:<14} '
            '| {volume_name:<20.20} '
            '| {rules:<20} |'.format(
                volume_id=volume.id,
                volume_name=volume_name,
                rules=rules))

    logger.info(
        '+----------------'
        '+----------------------'
        '+----------------------+')
