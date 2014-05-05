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
   * StatHat for logging.
   *
   * @var object
   */
  private $statHat;

 /**
   * Constructor for MBP_UserStatus
   *
   * @param array $credentials
   *   Secret settings from mb-secure-config.inc
   *
   * @param array $config
   *   Configuration settings from mb-config.inc
   */
  public function __construct($credentials, $config) {

    $this->config = $config;
    $this->credentials = $credentials;

    $this->MessageBroker = new MessageBroker($credentials, $config);
    $this->statHat = new StatHat(STATHAT_EZKEY, 'MBP_UserStatus->reportEmailErrors:');
    // @todo: Create enviroment variable in mc-config.inc to flag production
    $this->statHat->setIsProduction(TRUE);
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
    $this->statHat->reportCount(1);

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
      $this->statHat_resubscribeEmail_error = new StatHat(STATHAT_EZKEY, 'MBP_UserStatus->resubscribeEmail Error:');
      $this->statHat_resubscribeEmail_error->reportCount(1);
    }
    else {
      $this->statHat->addStatName('MBP_UserStatus->resubscribeEmail:');
      $this->statHat->reportCount(1);
    }

  }

}
