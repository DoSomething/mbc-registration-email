<?php
/**
 * MBC_RegistrationEmail_UserRegistration_Consumer:
 */

namespace DoSomething\MBC_RegistrationEmail;

use DoSomething\MB_Toolbox\MB_Configuration;
use DoSomething\StatHat\Client as StatHat;
use DoSomething\MB_Toolbox\MB_Toolbox;
use DoSomething\MB_Toolbox\MB_Toolbox_BaseConsumer;
use DoSomething\MB_Toolbox\MB_MailChimp;
use \Exception;

/**
 * MBC_RegistrationEmail_UserRegistration_Consumer class - .
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

  /*
   * The MailChimp global list ID.
   */
  const GLOBAL_MAILCHIMP_LIST_ID = '8e7844f6dd';

  /*
   * The MailChimp global list ID.
   */
  const UK_MAILCHIMP_LIST_ID = 'fd48935715';

  /**
   * A collection of tools used by all of the Message Broker applications.
   * @var object $mbToolbox
   */
   private $mbToolbox;

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
    $this->mbToolbox = $this->mbConfig->getProperty('mbToolbox');
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

    if ($this->canProcess($this->message)) {

      try {

        parent::logConsumption(['email']);
        $this->setter($this->message);
        $params = [
            'user_country' => $this->submission['user_country'],
            'mailchimp_list_id' => $this->submission['mailchimp_list_id']
        ];
        $this->process($params);
        $this->submission = [];
        $this->messageBroker->sendAck($this->message['payload']);
      }
      catch(Exception $e) {
        echo 'Error sending email address: ' . $this->message['email'] . ' to MailChimp for user signup. Error: ' . $e->getMessage();
        $this->statHat->ezCount('mbc-registration-email: MBC_RegistrationEmail_UserRegistration_Consumer: Exception', 1);

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
      $this->waitingSubmissions = [];
    }

    echo '-------  mbc-registration-email - MBC_RegistrationEmail_CampaignSignup_Consumer->consumeUserRegistrationQueue() END -------', PHP_EOL . PHP_EOL;
  }

  /**
   * Conditions to test before processing the message.
   *
   * @param array $message Values to use to determin if message can be processed.
   *
   * @return boolean
   */
  protected function canProcess($message) {

    if (!(isset($message['email']))) {
      echo '- canProcess(), email not set.', PHP_EOL;
      return FALSE;
    }

   if (filter_var($message['email'], FILTER_VALIDATE_EMAIL) === false) {
      echo '- canProcess(), failed FILTER_VALIDATE_EMAIL: ' . $message['email'], PHP_EOL;
      return FALSE;
    }
    else {
      $message['email'] = filter_var($message['email'], FILTER_VALIDATE_EMAIL);
    }

    // Exclude generated emails adresses.
    if (preg_match('/@.*\.import$/', $this->message['email'])) {
      echo '- canProcess(), import placeholder address: ' . $this->message['email'], PHP_EOL;
      return false;
    }

    if (!(isset($message['activity']))) {
      echo '- canProcess(), activity not set.', PHP_EOL;
      return FALSE;
    }
    if ($message['activity'] != 'user_register' &&
        $message['activity'] != 'vote' &&
        $message['activity'] != 'user_welcome-niche' &&
        $message['activity'] != 'user_welcome-teenlife' &&
        $message['activity'] != 'user_welcome-att-ichannel' &&
        $message['activity'] != 'user_import') {
      echo '- canProcess(), activity: ' . $message['activity'] . ' not "user_register","vote" or one of the user_import activities, skipping message.', PHP_EOL;
      return FALSE;
    }

    if (!(isset($message['mailchimp_list_id']))) {
      echo '- canProcess(), mailchimp_list_id not set.', PHP_EOL;
      return FALSE;
    }

    if (isset($message['birthdate_timestamp']) && ($message['birthdate_timestamp'] > time() - (60 * 60 * 24 * 365 * 13))) {
      echo '- canProcess(), user is 13 or under years old.', PHP_EOL;
      return FALSE;
    }

    if (isset($message['user_country']) && $message['user_country'] == 'CA') {
      echo '- canProcess(), user_country : CA, skip processing.', PHP_EOL;
      return FALSE;
    }
    if (isset($message['application_id']) && $message['application_id'] == 'CA') {
      echo '- canProcess(), application_id : CA, skip processing.', PHP_EOL;
      return FALSE;
    }

    if (!(isset($message['user_language']))) {
      echo '- canProcess(), WARNING: user_language not set.', PHP_EOL;
      parent::reportErrorPayload();
    }

    if (!(isset($message['user_country']))) {
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

      // Send UK based users to Global list.
      // $message['user_country'] = 'uk';
      $message['user_country'] = 'global';
    }

    // No longer put Brazil (br) or Mexico (mx) users into separate MailChimp lists. Add to global list.
    if ($message['user_country'] == 'br' || $message['user_country'] == 'mx') {
      echo '- User country: '. strtoupper($message['user_country']) . ' reset to global.', PHP_EOL;
      $this->submission['user_country'] = 'global';
    }
    // Extract user_country if not set or default to "US".
    elseif (!(isset($message['user_country'])) && isset($message['email_template'])) {
      $message['user_country'] = strtolower($this->mbToolbox->countryFromTemplateName($message['email_template']));
    }
    elseif (isset($message['user_country'])) {
       $this->submission['user_country'] = strtolower($message['user_country']);
    }
    else {
      $this->submission['user_country'] = 'global';
    }

    // Use mailchimp_list_id UK List if UK or GB user_country and mailchimp_list_id not defined
    if (!(isset($message['mailchimp_list_id'])) && (strtolower($this->submission['user_country']) == 'uk' || strtolower($this->submission['user_country']) == 'gb')) {
      // echo '- user_country: ' . strtolower($this->submission['user_country']) . ', assigning UK mailchimp_list_id.', PHP_EOL;
      // $this->submission['mailchimp_list_id'] = self::UK_MAILCHIMP_LIST_ID;

      echo '- user_country: ' . strtolower($this->submission['user_country']) . ', assigning Global mailchimp_list_id.', PHP_EOL;
      $this->submission['mailchimp_list_id'] = self::GLOBAL_MAILCHIMP_LIST_ID;
    }
    // No longer put Brazil (br) or Mexico (mx) users into separate MailChimp lists. Add to global list.
    elseif ($this->submission['user_country'] == 'br' || $this->submission['user_country'] == 'mx') {
      echo '- User BR or MX mailchimp_list_id adjusted to global.', PHP_EOL;
      $this->submission['mailchimp_list_id'] = self::GLOBAL_MAILCHIMP_LIST_ID;
    }
    elseif (isset($message['mailchimp_list_id'])) {
      $this->submission['mailchimp_list_id'] = $message['mailchimp_list_id'];
    }
    // Default to main US list if value not present
    else {
      echo '- WARNING: mailchimp_list_id not set, defaulting to global list.', PHP_EOL;
      $this->submission['mailchimp_list_id'] = self::GLOBAL_MAILCHIMP_LIST_ID;
    }
    if (!(isset($this->mbcURMailChimp[$this->submission['user_country']]))) {
      echo '- WARNING: mbcURMailChimp object for ' . strtoupper($this->submission['user_country']) . ' does not exist, defaulting to global list.', PHP_EOL;
      $this->submission['user_country'] = 'global';
      $this->submission['mailchimp_list_id'] = self::GLOBAL_MAILCHIMP_LIST_ID;
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
   *
   *  Add email and related message details grouped by country. The country defines which
   *  MailChimp object and related account to submit to.
   *
   *  @param array $params Values used for final processing.
   *
   *  @todo: Root out apps that are not setting the user_country and/or mailchimp_list_id
   *    - mbc-user-import, 20 Nov 2015
   */
  protected function process($params) {

    $country = $params['user_country'];
    $mailchimp_list_id = $params['mailchimp_list_id'];

    $this->waitingSubmissions[$country][$mailchimp_list_id][] = $this->submission;
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
          // Override MX and BR API key with US.
          // We don't need to use different API keys anymore because
          // we add BR and MX users to US "International" list.
          // The plan is to make sure nothing else is using BR and MX
          // API keys and remove this functionality for good.
          // For now, hardcoded override is the safest way to do it.
          if ($country !== 'global') {
            $country = 'us';
          }

          $this->mbcURMailChimp[$country]->addSubscribersToBatch($listID, $submissions);
          $responses = $this->mbcURMailChimp[$country]->commitBatch();

          $this->statHat->ezCount('mbc-registration-email: MBC_RegistrationEmail_UserRegistration_Consumer: processSubmissions', 1);
          if (empty($responses['success'])) {
            echo '- ERRORS enountered in MailChimp submission... processing.', PHP_EOL;
            $this->statHat->ezCount('mbc-registration-email: MBC_RegistrationEmail_UserRegistration_Consumer: processSubmissions: error_count > 0', 1);
            $processSubmissionErrors = new MBC_RegistrationEmail_SubmissionErrors($this->mbcURMailChimp[$country], $listID);
            $processSubmissionErrors->processSubmissionErrors($results['responses'], $composedBatch);
          }
        }
        catch(Exception $e) {
          echo 'Error: Failed to submit batch to ' . $country . ' MailChimp account. Error: ' . $e->getMessage(), PHP_EOL;
          $this->statHat->ezCount('mbc-registration-email: MBC_RegistrationEmail_UserRegistration_Consumer: processSubmissions: Exception', 1);
          $this->channel->basic_cancel($this->message['payload']->delivery_info['consumer_tag']);
        }
      }

    }
    $this->lastSubmissionStamp = time();
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
