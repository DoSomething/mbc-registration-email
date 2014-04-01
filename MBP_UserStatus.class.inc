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

    // Setup RabbitMQ connection
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

    return $status;

  }

}
