<?php
/**
 * mbc-registration-email_userSubscriptions
 *
 */

date_default_timezone_set('America/New_York');
define('CONFIG_PATH',  __DIR__ . '/messagebroker-config');
// The number of messages for the consumer to reserve with each callback
// See consumeMwessage for further details.
// Necessary for parallel processing when more than one consumer is running on the same queue.
define('QOS_SIZE', 1);

// Load up the Composer autoload magic
require_once __DIR__ . '/vendor/autoload.php';
use DoSomething\MBC_RegistrationEmail\MBC_RegistrationEmail_UserSubscriptions_Consumer;

require_once __DIR__ . '/mbc-registration-email_userSubscriptions.config.inc';

// Kick off
echo '------- mbc-registration-email_userSubscription START: ' . date('j D M Y G:i:s T') . ' -------', PHP_EOL;

$mb = $mbConfig->getProperty('messageBroker_Subscribes');
$mb->consumeMessage(array(new MBC_RegistrationEmail_UserSubscriptions_Consumer(), 'consumeUserMailchimpStatusQueue'), QOS_SIZE);

echo '-------mbc-registration-email_useerSubscriptions END: ' . date('j D M Y G:i:s T') . ' -------', PHP_EOL;
