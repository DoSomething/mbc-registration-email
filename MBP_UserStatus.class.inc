<?php
/**
 * MBP_UserStatus class - report changes in a user account status.
 */
class MBP_UserStatus
{

  /**
   * Message Broker object that details the connection to RabbitMQ.
   *
   * @var object
   */
  private $MessageBroker;

  /**
   * Collection of configuration settings.
   *
   * @var array
   */
  private $config;

  /**
   * Collection of secret connection settings.
   *
   * @var array
   */
  private $credentials;

  /**
   * Setting from external services - Mailchimp.
   *
   * @var array
   */
  private $settings;

 /**
   * Constructor for MBP_UserStatus
   *
   * @param array $credentials
   *   Secret settings from mb-secure-config.inc
   *
   * @param array $config
   *   Configuration settings from mb-config.inc
   */
  public function __construct($credentials, $config, $settings) {

    $this->credentials = $credentials;
    $this->config = $config;
    $this->settings = $settings;

    $this->MessageBroker = new MessageBroker($credentials, $config);
  }

  /**
   * Submit user errors to UserAPI
   *
   * @param array $errorDetails
   *   The list of errors reported to add to the user record
   *
   * @return string $status
   *   The results of the submission to the UserAPI
   */
  public function reportEmailErrors($errorDetails) {

    $payload = serialize($errorDetails);
    $status = $this->MessageBroker->publishMessage($payload);
    $statHat = new StatHat($this->$settings['stathat_ez_key'], 'MBP_UserStatus->reportEmailErrors');

    return $status;
  }

  /**
   * Resubscribe email address with interest group assignment - submit queue
   * entry to mailchimpCampaignSignupQueue
   *
   * @param array $errorDetails
   *   The error details reported when the email address was submitted as a part
   *   of a batch submission.
   *
   * @param array $composedBatch
   *   The details of the batch data sent to Mailchimp. The interest group
   *   details will be extractacted for the /lists/subscribe submission to
   *   Mailchimp
   *
   * @param array $settings
   *   Settings from external services - Mailchimp
   *
   * @return string $status
   *   The results of the submission to the UserAPI
   */
  public static function resubscribeEmail($errorDetails, $composedBatch, $settings) {

    // Lookup the group assignment details from $composedBatch by the email
    // address in $errorDetails
    foreach ($composedBatch as $composedItemCount => $composedItem) {
      if ($composedItem['email']['email'] == $errorDetails['email']['email']) {
        $resubscribeDetails = $composedItem;
        break;
      }
    }

    // Submit subscription to Mailchimp
    $mc = new \Drewm\MailChimp($settings['mailchimp_apikey']);

    $results = $mc->call("lists/subscribe", array(
      'id' => $settings['mailchimp_list_id'],
      'email' => array(
        'email' => $composedItem['email']['email']
        ),
      'merge_vars' => $composedItem['merge_vars'],
      'double_optin' => FALSE,
      'update_existing' => TRUE,
      'replace_interests' => FALSE,
      'send_welcome' => FALSE,
    ));

    if (isset($results['error'])) {
      $statHat = new StatHat($this->$settings['stathat_ez_key'], 'MBP_UserStatus->resubscribeEmail Error');
    }
    else {
      $statHat = new StatHat($this->$settings['stathat_ez_key'], 'MBP_UserStatus->resubscribeEmail');
    }

  }

}
