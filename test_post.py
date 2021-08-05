import unittest
import requests, json, datetime, csv, time
from apifacebook import ApiFacebook

class ApiRickTests(unittest.TestCase):
    def test_access(self):
        apifacebook = ApiFacebook('Comex','EAAFLWQphDb4BAJn4aABeUQHWQjsb6NmkOUO6Wvf9U7GZAdW30nqTLICxrI9pOyMtqfb2SG66ZAibnvI1BvCDyLdfCh4EDGckzQC4O5gVURtl6QyXwASxIlejyJnxgE0ZAMZCOf8QalMLDPTGmZCh8zK0LIcbx8ZCCjhgLYl1R5dwZDZD')
        self.assertEqual(apifacebook.access, 'EAAFLWQphDb4BAJn4aABeUQHWQjsb6NmkOUO6Wvf9U7GZAdW30nqTLICxrI9pOyMtqfb2SG66ZAibnvI1BvCDyLdfCh4EDGckzQC4O5gVURtl6QyXwASxIlejyJnxgE0ZAMZCOf8QalMLDPTGmZCh8zK0LIcbx8ZCCjhgLYl1R5dwZDZD')

    def test_getinbox(self):
        apifb = ApiFacebook('Comex','EAAFLWQphDb4BAJn4aABeUQHWQjsb6NmkOUO6Wvf9U7GZAdW30nqTLICxrI9pOyMtqfb2SG66ZAibnvI1BvCDyLdfCh4EDGckzQC4O5gVURtl6QyXwASxIlejyJnxgE0ZAMZCOf8QalMLDPTGmZCh8zK0LIcbx8ZCCjhgLYl1R5dwZDZD','141796229342716')
        procesar = apifb.get_procesar()
        self.assertEqual(procesar,True)

if __name__ == '__main__':
    unittest.main()