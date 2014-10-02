import sys 
import re

class Evaluator:

   def evaluate(self, rule, event, platform):

    try:
      ruleStr = rule.lower()

      # check if rule contains digits then type=iOS else type=Android 
      # _digits = re.compile('\d')

      # if bool(_digits.search(ruleStr)): # contains digits
      #   rule_type = 'i'
      # else:
      #   rule_type = "a"

      # print "rule type: " + rule_type

      rule_type = platform

      if "sendTime" in event:
        event_type = "i"
      else:
        event_type = "a"

      # print "event_type: " + event_type 

    ###### iOS
      if  event_type == "i" and rule_type == "i": 

        # print "i here "

        res = map(int, re.findall(r'\d+', ruleStr))
        for item in res:
          if item in event["appIds"]:
            ruleStr = ruleStr.replace(str(item), 'True')
          else:
            ruleStr = ruleStr.replace(str(item), 'False')

        # print ruleStr
        # e = eval(ruleStr)
        # print e
        return eval(ruleStr)
      


      ###### Android 
      if  event_type == "a" and (rule_type == "a"):    
        # print "Android.." 
        # print "1"
        splitted =  re.split('[(]|[)]| or| and| not',ruleStr)

        for item in splitted:

          item = item.strip()
          if item == '':
            continue
          if item in event["appIds"]:
            # print item
            ruleStr = ruleStr.replace(item, 'True')
          else:
            # print item 
            ruleStr = ruleStr.replace(item, 'False')
        # print ruleStr
        # e = eval(ruleStr)
        # print e 
        return eval(ruleStr)  
      # print ruleStr

      
    except:
      return False
    return False  
