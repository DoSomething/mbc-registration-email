<?php
/**
 * MBC_RegistrationEmail_UserRegistration_Consumer:  
 */

namespace DoSomething\MBC_RegistrationEmail;

use DoSomething\MB_Toolbox\MB_Configuration;
use DoSomething\MBStatTracker\StatHat;
use DoSomething\MB_Toolbox\MB_Toolbox_BaseConsumer;
use DoSomething\MB_Toolbox\MB_MailChimp;
use \Exception;

/**
 *MBC_RegistrationEmail_UserRegistration_Consumer class - .
 */
class MBC_RegistrationEmail_UserRegistration_Consumer extends MB_Toolbox_BaseConsumer
{

  /*
   * The amount of seconds to wait in an idle state before processing existing submissions even
   * if the batch size has not been reached.
   */
  const BATCH_SIZE = 50;

  /*
   * The amount of seconds to wait in an idle state before processing existing submissions even
   * if the batch size has not been reached.
   */
  const IDLE_TIME = 300;

  /**
   * MailChimp objects indexed by supported country codes.
   * @var array $mbcURMailChimp
   */
  protected $mbcURMailChimp;

  /**
   * One submission, compiled to make up a part of batch submission to MailChimp.
   */
  protected $submission = [];

  /**
   * Submissions to be sent to MailChimp, indexed by MailChimp API and email address.
   * @var array $waitingSubmissions
   */
  protected $waitingSubmissions;

  /**
   * Submissions to be sent to MailChimp, indexed by MailChimp API and email address.
   * @var array $waitingSubmissions
   */
  protected $lastSubmissionStamp;

  /**
   * Initialize MailChimp objects for each supported country.
   */
  public function __construct() {

    parent::__construct();
    $this->mbcURMailChimp = $this->mbConfig->getProperty('mbcURMailChimp_Objects');
    $this->submission = [];
    $this->waitingSubmissions = [];
    $this->lastSubmissionStamp = time();
  }

  /**
   * Callback for messages arriving in the UserRegistrationQueue.
   *
   * @param string $payload
   *   A seralized message to be processed.
   */
  public function consumeUserRegistrationQueue($payload) {

    echo '-------  mbc-registration-email - MBC_RegistrationEmail_CampaignSignup_Consumer->consumeUserRegistrationQueue() START -------', PHP_EOL;

    parent::consumeQueue($payload);
    $this->logConsumption('email');

    if ($this->canProcess()) {

      try {

        $this->setter($this->message);
        $this->process();
      }
      catch(Exception $e) {
        echo 'Error sending email address: ' . $this->message['email'] . ' to MailChimp for user signup. Error: ' . $e->getMessage();

        // @todo: Send copy of message to "dead message queue" with details of the original processing: date,
        // origin queue, processing app. The "dead messages" queue can be used to monitor health.
      }

    }
    else {
      echo '- ' . $this->message['email'] . ' can\'t be processed, removing from queue.', PHP_EOL;
      $this->messageBroker->sendAck($this->message['payload']);

      // @todo: Send copy of message to "dead message queue" with details of the original processing: date,
      // origin queue, processing app. The "dead messages" queue can be used to monitor health.
    }


    if ($this->canProcessSubmissions()) {

      $this->processSubmissions();
      echo '- unset $this->waitingSubmissions: ' . $this->waitingSubmissionsCount($this->waitingSubmissions), PHP_EOL . PHP_EOL;
      unset($this->waitingSubmissions);
    }

    echo '-------  mbc-registration-email - MBC_RegistrationEmail_CampaignSignup_Consumer->consumeUserRegistrationQueue() END -------', PHP_EOL . PHP_EOL;
  }

  /**
   * Conditions to test before processing the message.
   *
   * @return boolean
   */
  protected function canProcess() {

    if (!(isset($this->message['email']))) {
      echo '- canProcess(), email not set.', PHP_EOL;
      return FALSE;
    }

   if (filter_var($this->message['email'], FILTER_VALIDATE_EMAIL) === false) {
      echo '- canProcess(), failed FILTER_VALIDATE_EMAIL: ' . $this->message['email'], PHP_EOL;
      return FALSE;
    }
    else {
      $this->message['email'] = filter_var($this->message['email'], FILTER_VALIDATE_EMAIL);
    }

    if (!(isset($this->message['activity']))) {
      echo '- canProcess(), activity not set.', PHP_EOL;
      return FALSE;
    }
    if ($this->message['activity'] != 'user_register' &&
        $this->message['activity'] != 'vote' &&
        $this->message['activity'] != 'user_welcome-niche' &&
        $this->message['activity'] != 'user_welcome-teenlife' &&
        $this->message['activity'] != 'user_welcome-att-ichannel' &&
        $this->message['activity'] != 'user_import') {
      echo '- canProcess(), activity: ' . $this->message['activity'] . ' not "user_register","vote" or one of the user_import activities, skipping message.', PHP_EOL;
      return FALSE;
    }

    if (!(isset($this->message['mailchimp_list_id']))) {
      echo '- canProcess(), mailchimp_list_id not set.', PHP_EOL;
      return FALSE;
    }

    if (!(isset($this->message['email_template']))) {
      echo '- canProcess(), email_template not set.', PHP_EOL;
      return FALSE;
    }

    if (isset($this->message['birthdate_timestamp']) && ($this->message['birthdate_timestamp'] > time() - (60 * 60 * 24 * 365 * 13))) {
      echo '- canProcess(), user is 13 or under years old.', PHP_EOL;
      return FALSE;
    }

    if (isset($this->message['user_country']) && $this->message['user_country'] == 'CA') {
      echo '- canProcess(), user_country : CA, skip processing.', PHP_EOL;
      return FALSE;
    }
    if (isset($this->message['application_id']) && $this->message['application_id'] == 'CA') {
      echo '- canProcess(), application_id : CA, skip processing.', PHP_EOL;
      return FALSE;
    }

    if (!(isset($this->message['user_language']))) {
      echo '- canProcess(), WARNING: user_language not set.', PHP_EOL;
      parent::reportErrorPayload();
    }

    if (!(isset($this->message['user_country']))) {
      echo '- canProcess(), WARNING: user_country not set.', PHP_EOL;
      parent::reportErrorPayload();
    }

    return TRUE;
  }

  /**
   * Construct values for submission to email service.
   *
   * @param array $message
   *   The message to process based on what was collected from the queue being processed.
   */
  protected function setter($message) {

    $this->submission = [];
    $this->submission['email'] = $message['email'];

    // Deal with old affiliate sites and messages that do not have user_country set
    if ($message['application_id'] == 'GB' || $message['application_id'] == 'UK') {
      $message['user_country'] = 'uk';
    }

    // Extract user_country if not set or default to "US".
    if (!(isset($message['user_country'])) && isset($message['email_template'])) {
      $message['user_country'] = strtolower($this->countryFromTemplateName($message['email_template']));
    }
    elseif (isset($message['user_country'])) {
       $this->submission['user_country'] = strtolower($message['user_country']);
    }
    else {
      $this->submission['user_country'] = 'us';
    }

    // Default to main US list if value not present
    if (strtolower($this->submission['user_country']) == 'uk' || strtolower($this->submission['user_country']) == 'gb') {
      echo '- user_country: ' . strtolower($this->submission['user_country']) . ', assigning UK mailchimp_list_id.', PHP_EOL;
      $this->submission['mailchimp_list_id'] = 'fd48935715';
    }
    elseif (isset($message['mailchimp_list_id'])) {
      $this->submission['mailchimp_list_id'] = $message['mailchimp_list_id'];
    }
    else {
      echo '- WARNING: mailchimp_list_id not set, defaulting to general US list.', PHP_EOL;
      $this->submission['mailchimp_list_id'] = 'f2fab1dfd4';
    }
    if (!(isset($this->mbcURMailChimp[$this->submission['user_country']]))) {
      echo '- WARNING: mbcURMailChimp object for ' . $this->submission['user_country'] . ' does not exist, defaulting to global list.', PHP_EOL;
      $this->submission['user_country'] = 'global';
      $this->submission['mailchimp_list_id'] = '8e7844f6dd';
    }

    if (isset($message['user_language'])) {
      $this->submission['user_language'] = $message['user_language'];
    }

    if (isset($message['merge_vars']['FNAME'])) {
      $this->submission['fname'] = $message['merge_vars']['FNAME'];
    }
    if (isset($message['uid'])) {
      $this->submission['uid'] = $message['uid'];
    }
    if (isset($message['birthdate_timestamp'])) {
      $this->submission['birthdate_timestamp'] = (int)$message['birthdate_timestamp'];
    }
    elseif (isset($message['birthdate'])) {
      $this->submission['birthdate_timestamp'] = (int)$message['birthdate'];
    }
    if (isset($message['mobile'])) {
      $this->submission['mobile'] = $message['mobile'];
    }
    if (isset($message['source'])) {
      $this->submission['source'] = $message['source'];
    }
  }

  /**
   * process(): Gather message settings into waitingSubmissions array for batch processing.
   */
  protected function process() {

    // Add email and related message details grouped by country. The country defines which MailChimp
    // object and related account to submit to.

    // @todo: Root out apps that are not setting the user_country and/or mailchimp_list_id
    // - mbc-user-import, 20 Nov 2015

    $country = $this->submission['user_country'];
    $mailchimp_list_id = $this->submission['mailchimp_list_id'];
    $this->waitingSubmissions[$country][$mailchimp_list_id][] = $this->submission;
    unset($this->submission);
    $this->messageBroker->sendAck($this->message['payload']);
  }

  /**
   * processSubmissions(): Conditions to decide if current batch of users is ready for submission to MailChimp
   *
   * @return boolean
   */
  private function canProcessSubmissions() {

    // @todo: Throttle the number of consumers running. Based on the number of messages
    // waiting to be processed start / stop consumers. Make "reactive"!
    $queueMessages = parent::queueStatus('userRegistrationQueue');

    $waitingSubmissionsCount = $this->waitingSubmissionsCount($this->waitingSubmissions);
    echo '- waitingSubmissionsCount: ' . $waitingSubmissionsCount, PHP_EOL;
    if ($waitingSubmissionsCount >= self::BATCH_SIZE) {
      return TRUE;
    }

    // Are there message still to be processed
    if ($waitingSubmissionsCount != 0 && $waitingSubmissionsCount <= self::BATCH_SIZE) {
      $waitingSubmissions = TRUE;
    }
    else {
      $waitingSubmissions = FALSE;
    }
    // Idle time, process $waitingSubmissions if 5 minutes since last activity
    if ($queueMessages['ready'] == 0 && (($this->lastSubmissionStamp + self::IDLE_TIME) < time())) {
      $tiredOfWaiting = TRUE;
    }
    else {
      $tiredOfWaiting = FALSE;
    }
    if ($waitingSubmissions && $tiredOfWaiting) {
      return TRUE;
    }

    return FALSE;
  }

  /**
   * processSubmissions(): Submit contents of waiting submissions by country and lists within the countries.
   */
  protected function processSubmissions() {

    // Grouped by country and list_ids to define Mailchimp account and which list to subscribe to
    foreach ($this->waitingSubmissions as $country => $lists) {
      $country = strtolower($country);
      foreach ($lists as $listID => $submissions) {

        try {
          echo '-> submitting country: ' . $country, PHP_EOL;
          $composedBatch = $this->mbcURMailChimp[$country]->composeSubscriberSubmission($submissions);
          $results = $this->mbcURMailChimp[$country]->submitBatchSubscribe($listID, $composedBatch);
          if (isset($results['error_count']) && $results['error_count'] > 0) {
            echo '- ERRORS enountered in MailChimp submission... processing.', PHP_EOL;
            $processSubmissionErrors = new MBC_RegistrationEmail_SubmissionErrors($this->mbcURMailChimp[$country], $listID);
            $processSubmissionErrors->processSubmissionErrors($results['errors'], $composedBatch);
          }
        }
        catch(Exception $e) {
          echo 'Error: Failed to submit batch to ' . $country . ' MailChimp account. Error: ' . $e->getMessage(), PHP_EOL;
          $this->channel->basic_cancel($this->message['payload']->delivery_info['consumer_tag']);
        }
      }

    }
    $this->lastSubmissionStamp = time();
  }

  /**
   * countryFromTemplateName(): Extract country code from email template string. The last characters in string are
   * country specific. If last character is "-" the template name is invalid, default to "US" as country.
   *
   * @todo: Move method to MB_Toolbox class.
   *
   * @param string $emailTemplate
   *   The name of the template defined in the message transactional request.
   *
   * @return string $country
   *   A two letter country code.
   */
  protected function countryFromTemplateName($emailTemplate) {

    // Trap NULL values for country code. Ex: "mb-cgg2015-vote-"
    if (substr($emailTemplate, strlen($emailTemplate) - 1) == "-") {
      echo '- WARNING countryFromTemplateName() defaulting to country: US as template name was invalid. $emailTemplate: ' . $emailTemplate, PHP_EOL;
      $country = 'US';
    }
    else {
      $templateBits = explode('-', $emailTemplate);
      $country = $templateBits[count($templateBits) - 1];
    }

    return $country;
  }

  /**
   * waitingSubmissionsCount() - Calculate the total number of submissions ready to be batch submitted.
   *
   * @param array $waitingSubmissions
   *   User submissions grouped by country and list_id
   *
   * @return integer $count
   *   The total number of user records combined from all of the countries and their list_ids.
   */
  protected function waitingSubmissionsCount($waitingSubmissions = NULL) {

    $count = 0;
    foreach ($waitingSubmissions as $country => $list_id) {
      foreach ($list_id as $id => $signups) {
        $count += count($signups);
      }
    }

    return $count;
  }

  /**
   * logConsumption(): Extend to log the status of processing a specific message
   * element as well as the user_country and country.
   *
   * @param string $targetName
   */
  protected function logConsumption($targetName = NULL) {

    if (isset($this->message[$targetName]) && $targetName != NULL) {
      echo '** Consuming ' . $targetName . ': ' . $this->message[$targetName];
      if (isset($this->message['user_country'])) {
        echo ' from: ' .  $this->message['user_country'] . ' doing: ' . $this->message['activity'], PHP_EOL;
      } else {
        echo ', user_country not defined.', PHP_EOL;
      }
    } else {
      echo '- logConsumption tagetName: "' .$targetName . '" not defined.', PHP_EOL;
    }
  }

}
