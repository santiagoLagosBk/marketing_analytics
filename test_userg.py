from user_agents import parse

user_agent = 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 5.1; Trident/3.1)'
if __name__ == '__main__':

    user_agent = parse(user_agent)

    print(user_agent.browser.family)


    print(user_agent.os.family)
    print(user_agent.device.family)
    print(user_agent.device.brand)