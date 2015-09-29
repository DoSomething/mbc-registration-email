<?php
/**
 * MBC_RegistrationEmail_UserRegistration_Consumer:  
 */

namespace DoSomething\MBC_RegistrationEmail;

use DoSomething\MB_Toolbox\MB_Configuration;
use DoSomething\MBStatTracker\StatHat;
use DoSomething\MB_Toolbox\MB_Toolbox_BaseConsumer;
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
   *
   *
   * @param string $payload
   *   An JSON array of the details of the message to be sent
   */
  public function consumeUserRegistrationQueue($payload) {


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



  /**
   * Make signup submission to MailChimp
   *
   * @param string $mlid
   *   The MailChimp list id to connect to.
   * @param array $mAPIKey
   *   An array of emails and the MailChimp API key to submit the address to.
   * @param array $mbDeliveryTags
   *   A list of RabbitMQ delivery tags being processed in batch.
   *
   * @return array
   *   A list of the RabbitMQ queue entry IDs that have been successfully
   *   submitted to MailChimp.
   */
  private function submitToMailChimp($mAPIKey, $mlid, $composedBatch = array(), $mbDeliveryTags = array()) {

    $MailChimp = new \Drewm\MailChimp($mAPIKey);

    // Debugging
    // $results1 = $MailChimp->call("lists/list", array());
    // $results2 = $MailChimp->call("lists/interest-groupings", array('id' => 'f2fab1dfd4'));
    // $results3 = $MailChimp->call("lists/interest-groupings", array('id' => 'a27895fe0c'));

    // batchSubscribe($id, $batch, $double_optin=true, $update_existing=false, $replace_interests=true)
    // replace_interests: optional - flag to determine whether we replace the
    // interest groups with the updated groups provided, or we add the provided
    // groups to the member's interest groups (optional, defaults to true)
        // Lookup list details including "mailchimp_list_id"
    // -> 71893 "Do Something Members" is f2fab1dfd4 (who knows why?!?)
    $results['add_count'] = 0;
    $results['update_count'] = 0;

    $results = $MailChimp->call("lists/batch-subscribe", array(
      'id' => $mlid,
      'batch' => $composedBatch,
      'double_optin' => FALSE,
      'update_existing' => TRUE,
      'replace_interests' => FALSE,
    ));

    $status = '------- ' . date('D M j G:i:s:u T Y') . ' -------' . PHP_EOL;
    if ($results != 0) {
      if (isset($results['error_count']) && $results['error_count'] > 0) {
        $statusMessage = $this->updateUserMailchimpError($results['errors'], $mAPIKey, $mlid, $composedBatch);
        $status .= 'mbc-registration-email - submitToMailChimp(): Success with errors - ' . $statusMessage . PHP_EOL;
      }
      else {
        $status .= 'mbc-registration-email - submitToMailChimp(): Success!' . PHP_EOL;
      }

      if (!isset($results['add_count'])) {
        $results['add_count'] = 0;
      }
      if (!isset($results['update_count'])) {
        $results['update_count'] = 0;
      }
      $status .= ' [x] ' . $results['add_count'] . ' email addresses added / ' . $results['update_count'] . ' updated to ' . $mAPIKey . '.' . PHP_EOL;

      $this->statHat->clearAddedStatNames();
      $this->statHat->addStatName('submitToMailChimp-batch-subscribe add_count');
      $this->statHat->reportCount($results['add_count']);

      $this->statHat->clearAddedStatNames();
      $this->statHat->addStatName('submitToMailChimp-batch-subscribe update_count');
      $this->statHat->reportCount($results['update_count']);

    }
    else {
      // $this->channel->basic_recover(TRUE);
      $status .= 'Hmmm: No results returned from Mailchimp lists/batch-subscribe submisison.';
    }

    // Ack back submissions that have been processed
    foreach($mbDeliveryTags as $tagEmail => $mbDeliveryTag) {
      $this->channel->basic_ack($mbDeliveryTag);
    }

    return $status;
  }

  /**
   * A lists/batch-subscribe will report errors on any of the email addresses
   * in the batch. The errors will be logged in the UserAPI for use by other
   * parts of the Message Broker system.
   *
   * @param array $errors
   *   A list of RabbitMQ delivery tags being processed in batch.
   * @param array $mAPIKey
   *   An array of emails and the MailChimp API key to submit the address to.
   * @param string $mlid
   *   The MailChimp list id to connect to.
   *
   * @param array $composedBatch
   *  List or email address and their Mailchimp interest group assignments that
   *  were submitted as a batch.
   */
  private function updateUserMailchimpError($errors, $mAPIKey, $mlid, $composedBatch) {

    $resubscribes = 0;
    $failedResubscribes = 0;

    $config = array();
    $source = __DIR__ . '/messagebroker-config/mb_config.json';
    $mb_config = new MB_Configuration($source, $this->settings);
    $transactionalExchange = $mb_config->exchangeSettings('transactionalExchange');

    $config['exchange'] = array(
      'name' => $transactionalExchange->name,
      'type' => $transactionalExchange->type,
      'passive' => $transactionalExchange->passive,
      'durable' => $transactionalExchange->durable,
      'auto_delete' => $transactionalExchange->auto_delete,
    );
    foreach ($transactionalExchange->queues->userMailchimpStatusQueue->binding_patterns as $binding_key) {
      $config['queue'][] = array(
        'name' => $transactionalExchange->queues->userMailchimpStatusQueue->name,
        'passive' => $transactionalExchange->queues->userMailchimpStatusQueue->passive,
        'durable' =>  $transactionalExchange->queues->userMailchimpStatusQueue->durable,
        'exclusive' =>  $transactionalExchange->queues->userMailchimpStatusQueue->exclusive,
        'auto_delete' =>  $transactionalExchange->queues->userMailchimpStatusQueue->auto_delete,
        'bindingKey' => $binding_key,
      );
    }
    $config['routingKey'] = 'user.mailchimp.error';
    $mbError = new MessageBroker($this->credentials, $config);

    // Add extries for each error encountered to the directUserStatusExchange
    foreach ($errors as $errorDetails){

      // Resubscribe if email address is reported as unsubscribed - the
      // transaction / user signing up for a campaign is confirmation that
      // they want to resubscribe.
      if ($errorDetails['code'] == 212) {
        $resubscribeStatus = $this->resubscribeEmail($errorDetails, $mAPIKey, $mlid, $composedBatch);
        $resubscribeStatus ? $resubscribes++ : $failedResubscribes++;
      }
      else {
        $payload = serialize($errorDetails);
        $mbError->publishMessage($payload);

        $this->statHat->clearAddedStatNames();
        $this->statHat->addStatName('updateUserMailchimpError - Other');
        $this->statHat->reportCount(1);
      }

    }

    return 'resubscribes: ' . $resubscribes . ' - Failed Resubscribes: ' . $failedResubscribes;
  }

 /**
   * Resubscribe email address with interest group assignment - submit queue
   * entry to mailchimpCampaignSignupQueue
   *
   * @param array $errorDetails
   *   The error details reported when the email address was submitted as a part
   *   of a batch submission.
   *  @param string $mlid
   *   The MailChimp list id to connect to.
   * @param array $mAPIKey
   *   An array of emails and the MailChimp API key to submit the address to.
   * @param array $composedBatch
   *   The details of the batch data sent to Mailchimp. The interest group
   *   details will be extractacted for the /lists/subscribe submission to
   *   Mailchimp
   *
   * @return string $status
   *   The results of the submission to the UserAPI
   */
  private function resubscribeEmail($errorDetails, $mAPIKey, $mlid, $composedBatch) {

    // Lookup the group assignment details from $composedBatch by the email
    // address in $errorDetails
    foreach ($composedBatch as $composedItemCount => $composedItem) {
      if ($composedItem['email']['email'] == $errorDetails['email']['email']) {
        $resubscribeDetails = $composedItem;
        break;
      }
    }

    // Submit subscription to Mailchimp
    $mc = new \Drewm\MailChimp($mAPIKey);

    $results = $mc->call("lists/subscribe", array(
      'id' => $mlid,
      'email' => array(
        'email' => $composedItem['email']['email']
        ),
      'merge_vars' => $composedItem['merge_vars'],
      'double_optin' => FALSE,
      'update_existing' => TRUE,
      'replace_interests' => FALSE,
      'send_welcome' => FALSE,
    ));

    $this->statHat->clearAddedStatNames();
    if (isset($results['error'])) {
      $this->statHat->addStatName('updateUserMailchimpError-resubscribeEmail Error');
      $status = FALSE;
    }
    else {
      $this->statHat->addStatName('updateUserMailchimpError-resubscribeEmail');
      $status = TRUE;
    }
    $this->statHat->reportCount(1);

    return $status;
  }

}
