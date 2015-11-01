<?php
/**
 * mbc-registration-email_userRegistration
 *
 * Collect new user registrations from the userRegistrationQueue as a batch job.
 * A collection of entries will result in a batch submission to create
 * entries/accounts in MailChimp. The MailChimp accounts are used to build
 * segments for mass mailouts.
 */

date_default_timezone_set('America/New_York');
define('CONFIG_PATH',  __DIR__ . '/messagebroker-config');
// The number of messages for the consumer to reserve with each callback
// See consumeMwessage for further details.
// Necessary for parallel processing when more than one consumer is running on the same queue.
define('QOS_SIZE', 1);
define('BATCH_SIZE', 1);

// Load up the Composer autoload magic
require_once __DIR__ . '/vendor/autoload.php';
use DoSomething\MBC_RegistrationEmail\MBC_RegistrationEmail_UserRegistration_Consumer;

require_once __DIR__ . '/mbc-registration-email_userRegistration.config.inc';

// Kick off
echo '------- mbc-registration-email_userRegistration START: ' . date('j D M Y G:i:s T') . ' -------', PHP_EOL;

$mb = $mbConfig->getProperty('messageBroker');
$mb->consumeMessage(array(new MBC_RegistrationEmail_UserRegistration_Consumer(BATCH_SIZE), 'consumeUserRegistrationQueue'), QOS_SIZE);

echo '-------mbc-registration-email_useerRegistration END: ' . date('j D M Y G:i:s T') . ' -------', PHP_EOL;
