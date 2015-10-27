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

  /**
   * The number of email addresses to send in a batch submission to MailChimp.
   * @var array $batchSize
   */
  private $batchSize;

  /**
   * MailChimp objects indexed by supported country codes.
   * @var array $mbcURMailChimp
   */
  protected $mbcURMailChimp;

  /**
   * One submission to be complited as part of batch submission to MailChimp.
   */
  protected $submission = [];

  /**
   * Submissions to be sent to MailChimp, indexed by MailChimp API and email address.
   * @var array $waitingSubmissions
   */
  protected $waitingSubmissions = [];

  /**
   *
   */
  public function __construct($batchSize) {

    parent::__construct();
    $this->batchSize = $batchSize;
    $this->mbcURMailChimp = $this->mbConfig->getProperty('mbcURMailChimp_Objects');
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
    echo '** Consuming: ' . $this->message['email'], PHP_EOL;

    if ($this->canProcess()) {

      try {

        $this->setter($this->message);
        $this->process();
      }
      catch(Exception $e) {
        echo 'Error sending email address: ' . $this->message['email'] . ' to MailChimp for user signup. Error: ' . $e->getMessage();
        $this->messageBroker->sendAck($this->message['payload']);

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

    // @todo: Throttle the number of consumers running. Based on the number of messages
    // waiting to be processed start / stop consumers. Make "reactive"!
    $queueMessages = parent::queueStatus('userRegistrationQueue');
    echo '- queueMessages ready: ' . $queueMessages['ready'], PHP_EOL;
    echo '- queueMessages unacked: ' . $queueMessages['unacked'], PHP_EOL;

    $waitingSubmissionsCount = $this->waitingSubmissionsCount($this->waitingSubmissions);
    if ($waitingSubmissionsCount >= $this->batchSize ||
       ($waitingSubmissionsCount < $this->batchSize && $queueMessages['ready'] == 0)) {

      // Grouped by country and list_ids to define Mailchimp account and which list to subscribe to
      foreach ($this->waitingSubmissions as $country => $lists) {
        $country = strtolower($country);
        if (!(isset($this->mbcURMailChimp[$country]))) {
          $county = 'global';
        }
        foreach ($lists as $list_id => $submissions) {

          try {
            $composedBatch = $this->mbcURMailChimp[$country]->composeSubscriberSubmission($submissions);
            $results = $this->mbcURMailChimp[$country]->submitBatchToMailChimp($list_id, $composedBatch);
            echo '** ' . $results['add_count'] . ' added, ' . $results['update_count'] . ' updated and ' . $results['error_count'] . ' errors.', PHP_EOL . PHP_EOL;
          }
          catch(Exception $e) {
            echo 'Error: Failed to submit batch to ' . $country . ' MailChimp account. Error: ' . $e->getMessage(), PHP_EOL;
            $this->channel->basic_cancel($this->message['original']->delivery_info['consumer_tag']);
          }
          if (isset($results['error_count']) > 0) {
            // $processSubmissionErrors = new MBC_RegistrationEmail_SubmissionErrors($this->mbcURMailChimp[$country]);
            // $processSubmissionErrors->processSubmissionErrors($results['errors'], $composedBatch);

          }
        }
      }

      echo '- unset $this->waitingSubmissions: ' . count($this->waitingSubmissions), PHP_EOL . PHP_EOL;
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
    if (isset($this->message['activity']) && $this->message['activity'] != 'user_register') {
      echo '- canProcess(), activity: ' . $this->message['activity'] . ' not "user_register", skipping message.', PHP_EOL;
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

    if (isset($this->message['birthdate_timestamp']) && ($this->message['birthdate_timestamp'] < time() - (60 * 60 * 24 * 365 * 13))) {
      echo '- canProcess(), user user 13 years old.', PHP_EOL;
      return FALSE;
    }

    if (!(isset($this->message['user_language']))) {
      echo '- canProcess(), WARNING: user_language not set.', PHP_EOL;
    }

    if (!(isset($this->message['user_country']))) {
      echo '- canProcess(), WARNING: user_country not set.', PHP_EOL;
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

    // Extract user_country if not set or default to "US".
    if (!(isset($message['user_country'])) && isset($message['email_template'])) {
      $message['user_country'] = $this->countryFromTemplateName($message['email_template']);
    }
    elseif (isset($message['user_country'])) {
       $this->submission['user_country'] = $message['user_country'];
    }
    else {
      $message['user_country'] = 'US';
    }
    $this->submission['mailchimp_list_id'] = $message['mailchimp_list_id'];

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
    $this->waitingSubmissions[$this->submission['user_country']][$this->submission['mailchimp_list_id']][] = $this->submission;
    unset($this->submission);
  }

  /**
   * countryFromTemplateName(): Extract country code from email template string. The last characters in string are country specific.
   *
   * @param string $emailTemplate
   *   The name of the template defined in the message transactional request.
   *
   * @return string $country
   *   A two letter country code.
   */
  protected function countryFromTemplateName($emailTemplate) {

    $templateBits = explode('-', $emailTemplate);
    $country = $templateBits[count($templateBits) - 1];

    return $country;
  }

  /**
   *
   */
  protected function waitingSubmissionsCount($waitingSubmissions) {

    $count = 0;
    foreach ($waitingSubmissions as $country => $list_id) {
      foreach ($list_id as $id => $signups) {
        $count += count($signups);
      }
    }

    return $count;
  }

}
