import numpy as np


class LinearDraws:
    def __init__(self, loan_amount: float,
                 draw_percent: float,
                 loan_duration: int) -> None:
        self.loan_amount = loan_amount
        self.draw_percent = draw_percent
        self.loan_duration = loan_duration
        self.remaining_loan = loan_amount * (1-draw_percent)

    def calculate_draws(self) -> None:
        draw_schedule = [self.loan_amount*self.draw_percent]
        draw_per_period = self.remaining_loan / self.loan_duration
        linear_draws = [draw_per_period] * self.loan_duration
        index = list(range(1, self.loan_duration+1))
        draw_schedule.extend(linear_draws)
        return dict(zip(index, draw_schedule))


class NormalDistributionLoan:
    def __init__(self, loan_amount: float,
                 draw_percent: float,
                 loan_duration: int) -> None:
        self.loan_amount = loan_amount
        self.draw_percent = draw_percent
        self.loan_duration = loan_duration
        self.remaining_loan = loan_amount * (1 - draw_percent)

    def calculate_draws(self) -> None:
        draw_schedule = [self.loan_amount * self.draw_percent]
        mu, sigma, n = 0.0, 1.0, self.loan_duration
        normal_dist = np.sort(np.random.normal(mu, sigma, n))

        def normal(x: float, mu: float, sigma: int) -> list[float]:
            return (2. * np.pi * sigma ** 2.) ** -.5 * np.exp(
                -.5 * (x - mu) ** 2. / sigma ** 2.)

        y_values = normal(normal_dist, mu, sigma)
        index = list(range(1, self.loan_duration + 1))
        for y in y_values:
            draw = y*self.remaining_loan
            draw_schedule.append(draw)
            self.remaining_loan = self.remaining_loan - draw
        return dict(zip(index, draw_schedule))


test = NormalDistributionLoan(100000000, .15, 20)
print(test.calculate_draws())
