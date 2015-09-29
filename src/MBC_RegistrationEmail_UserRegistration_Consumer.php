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
   * The number of queue entries to process in each session
   */
  const BATCH_SIZE = 100;

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
    $queueMessages = parent::queueStatus('transactionalQueue');
    echo '- queueMessages ready: ' . $queueMessages['ready'], PHP_EOL;
    echo '- queueMessages unacked: ' . $queueMessages['unacked'], PHP_EOL;

    echo '-------  mbc-registration-email - MBC_RegistrationEmail_CampaignSignup_Consumer->consumeUserRegistrationQueue() END -------', PHP_EOL . PHP_EOL;

  }

  /**
   * Conditions to test before processing the message.
   *
   * @return boolean
   */
  protected function canProcess() {

    return TRUE;
  }

  /**
   * Construct values for submission to email service.
   *
   * @param array $message
   *   The message to process based on what was collected from the queue being processed.
   */
  protected function setter($message) {

  }

  /**
   * process(): Send composed settings to Mandrill to trigger transactional email message being sent.
   */
  protected function process() {

  }

  /**
   * Format email list to meet MailChimp API requirements for batchSubscribe
   *
   * @param array $newSubscribers
   *   The list of email address to be formatted
   *
   * @return array
   *   Array of email addresses formatted to meet MailChimp API requirements.
   */
  private function composeSubscriberSubmission($newSubscribers = array()) {

    $composedSubscriberList = array();
    $mbDeliveryTags = array();

    $composedSubscriberList = array();
    foreach ($newSubscribers as $newSubscriberCount => $newSubscriber) {

      if (isset($newSubscriber['birthdate']) && is_int($newSubscriber['birthdate']) && $newSubscriber['birthdate'] < (time() - (60 * 60 * 24 * 365 * 1))) {
        $newSubscriber['birthdate_timestamp'] = $newSubscriber['birthdate'];
      }
      if (isset($newSubscriber['mobile']) && strlen($newSubscriber['mobile']) < 8) {
        unset($newSubscriber['mobile']);
      }

      // support different merge_vars for US vs UK
      if (isset($newSubscriber['application_id']) && $newSubscriber['application_id'] == 'UK') {
        $mergeVars = array(
          'FNAME' => isset($newSubscriber['fname']) ? $newSubscriber['fname'] : '',
          'LNAME' => isset($newSubscriber['lname']) ? $newSubscriber['lname'] : '',
          'MERGE3' => isset($newSubscriber['birthdate_timestamp']) ? date('d/m/Y', $newSubscriber['birthdate_timestamp']) : '',
        );
      }
      // Don't add Canadian users to MailChimp
      elseif (isset($newSubscriber['application_id']) && $newSubscriber['application_id'] == 'CA') {
        $this->channel->basic_ack($newSubscriber['mb_delivery_tag']);
        break;
      }
      else {
        $mergeVars = array(
          'UID' => isset($newSubscriber['uid']) ? $newSubscriber['uid'] : '',
          'FNAME' => isset($newSubscriber['fname']) ? $newSubscriber['fname'] : '',
          'MMERGE3' => (isset($newSubscriber['fname']) && isset($newSubscriber['lname'])) ? $newSubscriber['fname'] . $newSubscriber['lname'] : '',
          'BDAY' => isset($newSubscriber['birthdate_timestamp']) ? date('m/d', $newSubscriber['birthdate_timestamp']) : '',
          'BDAYFULL' => isset($newSubscriber['birthdate_timestamp']) ? date('m/d/Y', $newSubscriber['birthdate_timestamp']) : '',
          'MMERGE7' => isset($newSubscriber['mobile']) ? $newSubscriber['mobile'] : '',
        );
      }

      // Dont add address of users under 13
      if (!isset($newSubscriber['birthdate_timestamp']) ||
          (isset($newSubscriber['birthdate_timestamp']) &&
          ($newSubscriber['birthdate_timestamp'] < time() - (60 * 60 * 24 * 365 * 13)))) {
        $composedSubscriberList[$newSubscriberCount] = array(
          'email' => array(
            'email' => $newSubscriber['email']
          ),
        );

        if (isset($newSubscriber['source'])) {
          $mergeVars['groupings'] = array(
            0 => array(
              'id' => 10657,  // DoSomething Memebers -> Import Source
              'groups' => array($newSubscriber['source'])
            ),
          );
        }
        $composedSubscriberList[$newSubscriberCount]['merge_vars'] = $mergeVars;

        $mbDeliveryTags[$newSubscriber['email']] = $newSubscriber['mb_delivery_tag'];
      }
      else {
        $this->channel->basic_ack($newSubscriber['mb_delivery_tag']);
      }
    }

    return array($composedSubscriberList, $mbDeliveryTags);
  }

}
