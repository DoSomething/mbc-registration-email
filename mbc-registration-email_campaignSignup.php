<?php
/**
 * mbc-registration-email_campaignSignup
 *
 * Collect new user registrations from the mailchimpCampaignSignupQueue as a batch job.
 * A collection of entries will result in a batch submission to create campaign (interest
 * group assignment) entries/accounts in MailChimp. The MailChimp accounts are used to build
 * segments for mass mail outs.
 */

use DoSomething\MBC_RegistrationEmail\MBC_RegistrationEmail_CampaignSignup_Consumer;

date_default_timezone_set('America/New_York');
define('CONFIG_PATH',  __DIR__ . '/messagebroker-config');
// The number of messages for the consumer to reserve with each callback
// See consumeMwessage for further details.
// Necessary for parallel processing when more than one consumer is running on the same queue.
define('QOS_SIZE', 1);

// Manage enviroment setting
if (isset($_GET['environment']) && allowedEnvironment($_GET['environment'])) {
    define('ENVIRONMENT', $_GET['environment']);
} elseif (isset($argv[1])&& allowedEnvironment($argv[1])) {
    define('ENVIRONMENT', $argv[1]);
} elseif ($env = loadConfig()) {
    echo 'environment.php exists, ENVIRONMENT defined as: ' . ENVIRONMENT, PHP_EOL;
} elseif (allowedEnvironment('local')) {
    define('ENVIRONMENT', 'local');
}

// Load up the Composer autoload magic
require_once __DIR__ . '/vendor/autoload.php';

require_once __DIR__ . '/mbc-registration-email_campaignSignup.config.inc';

// Kick off
echo '------- mbc-registration-email_campaignSignup START: ' . date('j D M Y G:i:s T') . ' -------', PHP_EOL;

$mb = $mbConfig->getProperty('messageBroker');
$mb->consumeMessage(array(new MBC_RegistrationEmail_CampaignSignup_Consumer(), 'consumeMailchimpCampaignSignupQueue'), QOS_SIZE);

echo '-------mbc-registration-email_campaignSignup END: ' . date('j D M Y G:i:s T') . ' -------', PHP_EOL;

/**
 * Test if environment setting is a supported value.
 *
 * @param string $setting Requested environment setting.
 *
 * @return boolean
 */
function allowedEnvironment($setting)
{

    $allowedEnvironments = [
        'local',
        'dev',
        'prod',
        'thor',
    ];

    if (in_array($setting, $allowedEnvironments)) {
        return true;
    }

    return false;
}

/**
 * Gather configuration settings for current application environment.
 *
 * @return boolean
 */
function loadConfig() {

    // Check that environment config file exists
    if (!file_exists ('environment.php')) {
        return false;
    }
    include('./environment.php');

    return true;
}
